package io.github.streamingwithflink.chapter8

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import io.github.streamingwithflink.chapter8.util.{DerbyReader, DerbySetup}
import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Example program that emits sensor readings with UPSERT writes to an embedded in-memory
  * Apache Derby database.
  *
  * A separate thread queries the database every 10 seconds and prints the result.
  */
object IdempotentSinkFunctionExample {

  def main(args: Array[String]): Unit = {

    // setup the embedded Derby database
    DerbySetup.setupDerby(
      """
        |CREATE TABLE Temperatures (
        |  sensor VARCHAR(16) PRIMARY KEY,
        |  temp DOUBLE)
      """.stripMargin)
    // start a thread that prints the data written to Derby every 10 seconds.
    new Thread(
        new DerbyReader("SELECT sensor, temp FROM Temperatures ORDER BY sensor", 10000L))
      .start()

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

    val celsiusReadings: DataStream[SensorReading] = sensorData
      // convert Fahrenheit to Celsius using an inlined map function
      .map( r => SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )

    // write the converted sensor readings to Derby.
    celsiusReadings.addSink(new DerbyUpsertSink)

    env.execute()
  }
}

/**
  * Sink that upserts SensorReadings into a Derby table that is keyed on the sensor id.
  *
  * Since Derby does not feature a dedicated UPSERT command, we execute an UPDATE statement first
  * and execute an INSERT statement if UPDATE did not modify any row.
  */
class DerbyUpsertSink extends RichSinkFunction[SensorReading] {

  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // connect to embedded in-memory Derby
    val props = new Properties()
    conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", props)
    // prepare insert and update statements
    insertStmt = conn.prepareStatement(
      "INSERT INTO Temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement(
      "UPDATE Temperatures SET temp = ? WHERE sensor = ?")
  }

  override def invoke(r: SensorReading, context: Context[_]): Unit = {
    // set parameters for update statement and execute it
    updateStmt.setDouble(1, r.temperature)
    updateStmt.setString(2, r.id)
    updateStmt.execute()
    // execute insert statement if update statement did not update any row
    if (updateStmt.getUpdateCount == 0) {
      // set parameters for insert statement
      insertStmt.setString(1, r.id)
      insertStmt.setDouble(2, r.temperature)
      // execute insert statement
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
