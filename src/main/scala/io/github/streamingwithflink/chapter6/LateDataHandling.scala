package io.github.streamingwithflink.chapter6

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object LateDataHandling {

  // define a side output tag
  val lateReadingsOutput: OutputTag[SensorReading] =
    new OutputTag[SensorReading]("late-readings")

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(500L)

    // ingest sensor stream and shuffle timestamps to produce out-of-order records
    val outOfOrderReadings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // shuffle timestamps by max 7 seconds to generate late data
      .map(new TimestampShuffler(7 * 1000))
      // assign timestamps and watermarks with an offset of 5 seconds
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // Different strategies to handle late records.
    // Select and uncomment on of the lines below to demonstrate a strategy.

    // 1. Filter out late readings (to a side output) using a ProcessFunction
    filterLateReadings(outOfOrderReadings)
    // 2. Redirect late readings to a side output in a window operator
//    sideOutputLateEventsWindow(outOfOrderReadings)
    // 3. Update results when late readings are received in a window operator
//    updateForLateEventsWindow(outOfOrderReadings)

    env.execute()
  }

  /** Filter late readings to a side output and print the on-time and late streams. */
  def filterLateReadings(readings: DataStream[SensorReading]): Unit = {
    // re-direct late readings to the side output
    val filteredReadings: DataStream[SensorReading] = readings
      .process(new LateReadingsFilter)

    // retrieve late readings
    val lateReadings: DataStream[SensorReading] = filteredReadings
      .getSideOutput(lateReadingsOutput)

    // print the filtered stream
    filteredReadings.print()

    // print messages for late readings
    lateReadings
      .map(r => "*** late reading *** " + r.id)
      .print()
  }

  /** Count reading per tumbling window and emit late readings to a side output.
    * Print results and late events. */
  def sideOutputLateEventsWindow(readings: DataStream[SensorReading]): Unit = {

    val countPer10Secs: DataStream[(String, Long, Int)] = readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      // emit late readings to a side output
      .sideOutputLateData(lateReadingsOutput)
      // count readings per window
      .process(new ProcessWindowFunction[SensorReading, (String, Long, Int), String, TimeWindow] {

        override def process(
            id: String,
            ctx: Context,
            elements: Iterable[SensorReading],
            out: Collector[(String, Long, Int)]): Unit = {
          val cnt = elements.count(_ => true)
          out.collect((id, ctx.window.getEnd, cnt))
        }
      })

    // retrieve and print messages for late readings
    countPer10Secs
      .getSideOutput(lateReadingsOutput)
      .map(r => "*** late reading *** " + r.id)
      .print()

    // print results
    countPer10Secs.print()
  }

  /** Count reading per tumbling window and update results if late readings are received.
    * Print results. */
  def updateForLateEventsWindow(readings: DataStream[SensorReading]): Unit = {

    val countPer10Secs: DataStream[(String, Long, Int, String)] = readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      // process late readings for 5 additional seconds
      .allowedLateness(Time.seconds(5))
      // count readings and update results if late readings arrive
      .process(new UpdatingWindowCountFunction)

    // print results
    countPer10Secs
      .print()
  }
}

/** A ProcessFunction that filters out late sensor readings and re-directs them to a side output */
class LateReadingsFilter extends ProcessFunction[SensorReading, SensorReading] {

  override def processElement(
      r: SensorReading,
      ctx: ProcessFunction[SensorReading, SensorReading]#Context,
      out: Collector[SensorReading]): Unit = {

    // compare record timestamp with current watermark
    if (r.timestamp < ctx.timerService().currentWatermark()) {
      // this is a late reading => redirect it to the side output
      ctx.output(LateDataHandling.lateReadingsOutput, r)
    } else {
      out.collect(r)
    }
  }
}

/** A counting WindowProcessFunction that distinguishes between first results and updates. */
class UpdatingWindowCountFunction
    extends ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow] {

  override def process(
      id: String,
      ctx: Context,
      elements: Iterable[SensorReading],
      out: Collector[(String, Long, Int, String)]): Unit = {

    // count the number of readings
    val cnt = elements.count(_ => true)

    // state to check if this is the first evaluation of the window or not.
    val isUpdate = ctx.windowState.getState(
      new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean]))

    if (!isUpdate.value()) {
      // first evaluation, emit first result
      out.collect((id, ctx.window.getEnd, cnt, "first"))
      isUpdate.update(true)
    } else {
      // not the first evaluation, emit an update
      out.collect((id, ctx.window.getEnd, cnt, "update"))
    }
  }
}

/** A MapFunction to shuffle (up to a max offset) the timestamps of SensorReadings to produce out-of-order events. */
class TimestampShuffler(maxRandomOffset: Int) extends MapFunction[SensorReading, SensorReading] {

  lazy val rand: Random = new Random()

  override def map(r: SensorReading): SensorReading = {
    val shuffleTs = r.timestamp + rand.nextInt(maxRandomOffset)
    SensorReading(r.id, shuffleTs, r.temperature)
  }
}
