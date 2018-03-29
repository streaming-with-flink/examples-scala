package io.github.streamingwithflink.chapter7

import java.util

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object OperatorListStateFunction {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val highTempCounts: DataStream[(Int, Long)] = sensorData.flatMap(new HighTempCounterOpState(120.0))

    // print result stream to standard out
    highTempCounts.print()

    // execute application
    env.execute("Count high temperatures")
    }
}

/**
  * Counts per parallel instance of the function how many temperature readings exceed the configured threshold.
  *
  * @param threshold The high temperature threshold.
  */
class HighTempCounterOpState(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (Int, Long)]
    with ListCheckpointed[java.lang.Long] {

  // index of the subtask
  private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask
  // local count variable
  private var highTempCnt = 0L

  override def flatMap(in: SensorReading, out: Collector[(Int, Long)]): Unit = {
    if (in.temperature > threshold) {
      // increment counter if threshold is exceeded
      highTempCnt += 1
      // emit update with subtask index and counter
      out.collect((subtaskIdx, highTempCnt))
    }
  }

  override def restoreState(state: util.List[java.lang.Long]): Unit = {
    highTempCnt = 0
    // restore state by adding all longs of the list
    for (cnt <- state.asScala) {
      highTempCnt += cnt
    }
  }

  override def snapshotState(chkpntId: Long, ts: Long): java.util.List[java.lang.Long] = {
    // snapshot state as list with a single count
    java.util.Collections.singletonList(highTempCnt)
  }

  /** Split count into 10 partial counts for improved state distribution. */
//  override def snapshotState(chkpntId: Long, ts: Long): java.util.List[java.lang.Long] = {
//    // split count into ten partial counts
//    val div = highTempCnt / 10
//    val mod = (highTempCnt % 10).toInt
//    (List.fill(mod)(new java.lang.Long(div + 1)) ++ List.fill(10 - mod)(new java.lang.Long(div))).asJava
//  }

}