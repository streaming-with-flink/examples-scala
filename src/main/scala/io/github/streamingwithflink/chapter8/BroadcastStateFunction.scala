package io.github.streamingwithflink.chapter8

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object BroadcastStateFunction {

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

    // define a stream of thresholds
    val thresholds: DataStream[ThresholdUpdate] = env.fromElements(
      ThresholdUpdate("sensor_1", 5.0d),
      ThresholdUpdate("sensor_2", 2.0d),
      ThresholdUpdate("sensor_1", 1.2d))

    val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)

    val broadcastStateDescriptor =
      new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])
    val broadcastThresholds: BroadcastStream[ThresholdUpdate] = thresholds
      .broadcast(broadcastStateDescriptor)

    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
      .connect(broadcastThresholds)
      .process(new UpdatableTemperatureAlertFunction(4.0d))

    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
    }
}

case class ThresholdUpdate(id: String, threshold: Double)

/**
  * The function emits an alert if the temperature measurement of a sensor increased by more than
  * a threshold compared to the last reading. The thresholds are configured per sensor by a separate stream.
  * If no dedicated threshold is configured for a sensor, a default threshold is applied.
  *
  * @param defaultThreshold The default threshold to raise an alert.
  */
class UpdatableTemperatureAlertFunction(val defaultThreshold: Double)
    extends KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)] {

  private lazy val thresholdStateDescriptor =
    new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

  // the state handle object
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // create state descriptor
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    // obtain the state handle
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)

    // TODO: This is probably a bug that should be fixed in Flink
    thresholdStateDescriptor.initializeSerializerUnlessSet(getRuntimeContext.getExecutionConfig)
  }

  override def processBroadcastElement(
      update: ThresholdUpdate,
      keyedCtx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#KeyedContext,
      out: Collector[(String, Double, Double)]): Unit = {

    val thresholds = keyedCtx.getBroadcastState(thresholdStateDescriptor)

    if (update.threshold >= 1.0d) {
      // configure a new threshold of the sensor
      thresholds.put(update.id, update.threshold)
    } else {
      // remove sensor specific threshold
      thresholds.remove(update.id)
    }
  }

  override def processElement(
      reading: SensorReading,
      keyedReadOnlyCtx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#KeyedReadOnlyContext,
      out: Collector[(String, Double, Double)]): Unit = {

    // get read-only broadcast state
    val thresholds = keyedReadOnlyCtx.getBroadcastState(thresholdStateDescriptor)
    // get threshold for sensor
    val sensorThreshold: Double = if (thresholds.contains(reading.id)) thresholds.get(reading.id) else defaultThreshold

    // fetch the last temperature from state
    val lastTemp = lastTempState.value()
    // check if we need to emit an alert
    if (lastTemp > 0.0d && (reading.temperature / lastTemp) > sensorThreshold) {
      // temperature increased by more than the threshold
      out.collect((reading.id, reading.temperature, lastTemp))
    }

    // update lastTemp state
    this.lastTempState.update(reading.temperature)
  }
}
