package io.github.streamingwithflink.chapter7

import java.util.concurrent.CompletableFuture

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object TrackMaximumTemperature {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

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

    val tenSecsMaxTemps: DataStream[(String, Double)] = sensorData
      // project to sensor id and temperature
      .map(r => (r.id, r.temperature))
      // compute every 10 seconds the max temperature per sensor
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .max(1)

    // store latest value for each sensor in a queryable state
    tenSecsMaxTemps
      .keyBy(_._1)
      .asQueryableState("maxTemperature")

    // execute application
    env.execute("Track max temperature")
  }
}

object TemperatureDashboard {

  // queryable state proxy connection information.
  // can be looked up in logs of running QueryableStateJob
  val proxyHost = "127.0.0.1"
  val proxyPort = 9069
  // jobId of running QueryableStateJob.
  // can be looked up in logs of running job or the web UI
  val jobId = "d2447b1a5e0d952c372064c886d2220a"

  // how many sensors to query
  val numSensors = 5
  // how often to query
  val refreshInterval = 10000

  def main(args: Array[String]): Unit = {

    // configure client with host and port of queryable state proxy
    val client = new QueryableStateClient(proxyHost, proxyPort)

    val futures = new Array[CompletableFuture[ValueState[(String, Double)]]](numSensors)
    val results = new Array[Double](numSensors)

    // print header line of dashboard table
    val header = (for (i <- 0 until numSensors) yield "sensor_" + (i + 1)).mkString("\t| ")
    println(header)

    // loop forever
    while (true) {

      // send out async queries
      for (i <- 0 until numSensors) {
        futures(i) = queryState("sensor_" + (i + 1), client)
      }
      // wait for results
      for (i <- 0 until numSensors) {
        results(i) = futures(i).get().value()._2
      }
      // print result
      val line = results.map(t => f"$t%1.3f").mkString("\t| ")
      println(line)

      // wait to send out next queries
      Thread.sleep(refreshInterval)
    }

    client.shutdownAndWait()

  }

  def queryState(key: String, client: QueryableStateClient): CompletableFuture[ValueState[(String, Double)]] = {
    client.getKvState[String, ValueState[(String, Double)], (String, Double)](
      JobID.fromHexString(jobId),
      "maxTemperature",
      key,
      Types.STRING,
      new ValueStateDescriptor[(String, Double)]("", Types.TUPLE[(String, Double)]))
  }

}

