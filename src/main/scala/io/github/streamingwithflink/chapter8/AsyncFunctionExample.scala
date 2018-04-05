package io.github.streamingwithflink.chapter8

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import io.github.streamingwithflink.chapter8.util.{DerbySetup, DerbyWriter}
import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

/**
  * Example program that demonstrates the use of an AsyncFunction to enrich records with data
  * that is stored in an external database. The AsyncFunction queries the database via its
  * JDBC interface. For this demo, the database is an embedded, in-memory Derby database.
  *
  * The AsyncFunction sends queries and handles their results asynchronously in separate threads
  * for improved latency and throughput.
  *
  * The program includes a MapFunction that performs the same logic as the AsyncFunction in a
  * synchronous fashion. You can compare the behavior of the synchronous MapFunction and the
  * AsyncFunction by commenting out parts of the code.
  */
object AsyncFunctionExample {

  def main(args: Array[String]): Unit = {

    // setup the embedded Derby database
    DerbySetup.setupDerby(
      """CREATE TABLE SensorLocations (
        |  sensor VARCHAR(16) PRIMARY KEY,
        |  room VARCHAR(16))
      """.stripMargin)

    // insert some initial data
    DerbySetup.initializeTable(
      "INSERT INTO SensorLocations (sensor, room) VALUES (?, ?)",
      (1 to 80).map(i => Array(s"sensor_$i", s"room_${i % 10}")).toArray
        .asInstanceOf[Array[Array[Any]]]
    )

    // start a thread that updates the data of Derby table
    new Thread(new DerbyWriter(
      "UPDATE SensorLocations SET room = ? WHERE sensor = ?",
      (rand: Random) =>
        Array(s"room_${1 + rand.nextInt(20)}", s"sensor_${1 + rand.nextInt(80)}"),
      500L
    )).start()

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource).setParallelism(8)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // OPTION 1 (comment out to disable)
    // --------
    // look up the location of a sensor from a Derby table with asynchronous requests.
    val sensorLocations: DataStream[(String, String)] = AsyncDataStream
      .orderedWait(
        readings,
        new DerbyAsyncFunction,
        5, TimeUnit.SECONDS,        // timeout requests after 5 seconds
        100)                        // at most 100 concurrent requests

    // OPTION 2 (uncomment to enable)
    // --------
    // look up the location of a sensor from a Derby table with synchronous requests.
//    val sensorLocations: DataStream[(String, String)] = sensorData
//      .map(new DerbySyncFunction)

    // print the sensor locations
    sensorLocations.print()

    env.execute()
  }
}

/**
  * AsyncFunction that queries a Derby table via JDBC in a non-blocking fashion.
  *
  * Since the JDBC interface does not support asynchronous queries, starts individual threads to
  * concurrently query Derby and handle the query results in an non-blocking fashion.
  */
class DerbyAsyncFunction extends AsyncFunction[SensorReading, (String, String)] {

  // caching execution context used to handle the query threads
  private lazy val cachingPoolExecCtx =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  // direct execution context to forward result future to callback object
  private lazy val directExecCtx =
    ExecutionContext.fromExecutor(
      org.apache.flink.runtime.concurrent.Executors.directExecutor())

  /** Executes JDBC query in a thread and handles the resulting Future
    * with an asynchronous callback. */
  override def asyncInvoke(
      reading: SensorReading,
      resultFuture: ResultFuture[(String, String)]): Unit = {

    val sensor = reading.id

    // get room from Derby table as Future
    val room: Future[String] = Future {
      // Creating a new connection and statement for each record.
      // Note: This is NOT best practice!
      // Connections and prepared statements should be cached.
      val conn = DriverManager
        .getConnection("jdbc:derby:memory:flinkExample", new Properties())
      val query = conn.createStatement()

      // submit query and wait for result. this is a synchronous call.
      val result = query.executeQuery(
        s"SELECT room FROM SensorLocations WHERE sensor = '$sensor'")

      // get room if there is one
      val room = if (result.next()) {
        result.getString(1)
      } else {
        "UNKNOWN ROOM"
      }

      // close resultset, statement, and connection
      result.close()
      query.close()
      conn.close()

      // sleep to simulate (very) slow requests
      Thread.sleep(2000L)

      // return room
      room
    }(cachingPoolExecCtx)

    // apply result handling callback on the room future
    room.onComplete {
      case Success(r) => resultFuture.complete(Seq((sensor, r)))
      case Failure(e) => resultFuture.completeExceptionally(e)
    }(directExecCtx)
  }
}

/**
  * MapFunction that queries a Derby table via JDBC in a blocking fashion.
  */
class DerbySyncFunction extends RichMapFunction[SensorReading, (String, String)] {

  var conn: Connection = _
  var query: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // connect to Derby and prepare query
    this.conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
    this.query = conn.prepareStatement("SELECT room FROM SensorLocations WHERE sensor = ?")
  }

  override def map(reading: SensorReading): (String, String) = {

    val sensor = reading.id

    // set query parameter and execute query
    query.setString(1, sensor)
    val result = query.executeQuery()

    // get room if there is one
    val room = if (result.next()) {
      result.getString(1)
    } else {
      "UNKNOWN ROOM"
    }
    result.close()

    // sleep to simulate (very) slow requests
    Thread.sleep(2000L)

    // return sensor with looked up room
    (sensor, room)
  }

  override def close(): Unit = {
    super.close()
    this.query.close()
    this.conn.close()
  }
}
