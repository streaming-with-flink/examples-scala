package io.github.streamingwithflink.chapter7

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object KeyedStateFunction {

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

    val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)

    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
      .flatMap(new TemperatureAlertFunction(1.7))

    /* Scala shortcut to define a stateful FlatMapFunction. */
//    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
//      .flatMapWithState[(String, Double, Double), Double] {
//        case (in: SensorReading, None) =>
//          // no previous temperature defined. Just update the last temperature
//          (List.empty, Some(in.temperature))
//        case (r: SensorReading, lastTemp: Some[Double]) =>
//          // compare temperature difference with threshold
//          val tempDiff = (r.temperature - lastTemp.get).abs
//          if (tempDiff > 1.7) {
//            // threshold exceeded. Emit an alert and update the last temperature
//            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
//          } else {
//            // threshold not exceeded. Just update the last temperature
//            (List.empty, Some(r.temperature))
//          }
//      }

    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }
}

/**
  * The function emits an alert if the temperature measurement of a sensor changed by more than
  * a configured threshold compared to the last reading.
  *
  * @param threshold The threshold to raise an alert.
  */
class TemperatureAlertFunction(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  // the state handle object
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // create state descriptor
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    // obtain the state handle
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }

  override def flatMap(reading: SensorReading, out: Collector[(String, Double, Double)]): Unit = {

    // fetch the last temperature from state
    val lastTemp = lastTempState.value()
    // check if we need to emit an alert
    val tempDiff = (reading.temperature - lastTemp).abs
    if (tempDiff > threshold) {
      // temperature changed by more than the threshold
      out.collect((reading.id, reading.temperature, tempDiff))
    }

    // update lastTemp state
    this.lastTempState.update(reading.temperature)
  }
}
