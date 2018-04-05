/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.streamingwithflink.chapter8

import java.lang.Iterable
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

import io.github.streamingwithflink.chapter8.util.FailingMapper
import io.github.streamingwithflink.util.{ResettableSensorSource, SensorReading, SensorTimeAssigner}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.{CheckpointCommitter, GenericWriteAheadSink}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Example program that demonstrates the behavior of a write-ahead log sink that prints to the
  * standard output.
  *
  * The write-ahead sink aims prevents duplicate writes the most common failure cases.
  * However, there are failure scenarios in which records may be emitted more than once.
  * The write-ahead sink writes records when a checkpoint completes, i.e., in the configured
  * checkpoint interval.
  *
  * The program includes a MapFunction that throws an exception in regular intervals to simulate
  * application failures.
  * You can compare the behavior of a write-ahead sink and the regular print sink in failure cases.
  *
  * - The StdOutWriteAheadSink writes to the standard output when a checkpoint completes and
  * prevents duplicated result output.
  * - The regular print() sink writes to the standard output when a result is produced and
  * duplicates result output in case of a failure.
  *
  */
object WriteAheadSinkExample {

  def main(args: Array[String]): Unit = {

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
      .addSource(new ResettableSensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // compute average temperature of all sensors every second
    val avgTemp: DataStream[(String, Double)] = sensorData
      .timeWindowAll(Time.seconds(1))
      .apply((w, vals, out: Collector[(String, Double)]) => {
          val avgTemp = vals.map(_.temperature).sum / vals.count(_ => true)
          // format window timestamp as ISO timestamp string
          val epochSeconds = w.getEnd / 1000
          val tString = LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          // emit record
          out.collect((tString, avgTemp))
        }
      )
      // generate failures to trigger job recovery
      .map(new FailingMapper[(String, Double)](16)).setParallelism(1)

    // OPTION 1 (comment out to disable)
    // --------
    // print to standard out with a write-ahead log.
    // results are printed when a checkpoint is completed.
    avgTemp.transform(
        "WriteAheadSink",
        new StdOutWriteAheadSink)
      // enforce sequential writing
      .setParallelism(1)

    // OPTION 2 (uncomment to enable)
    // --------
    // print to standard out without write-ahead log.
    // results are printed as they are produced and re-emitted in case of a failure.
//    avgTemp.print()
//      // enforce sequential writing
//      .setParallelism(1)

    env.execute()
  }
}

/**
  * Write-ahead sink that prints to standard out and commits checkpoints to the local file system.
  */
class StdOutWriteAheadSink extends GenericWriteAheadSink[(String, Double)](
    // CheckpointCommitter that commits checkpoints to the local file system
    new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
    // Serializer for records
    createTypeInformation[(String, Double)].createSerializer(new ExecutionConfig),
    // Random JobID used by the CheckpointCommitter
    UUID.randomUUID.toString) {

  override def sendValues(
      readings: Iterable[(String, Double)],
      checkpointId: Long,
      timestamp: Long): Boolean = {

    for (r <- readings.asScala) {
      // write record to standard out
      println(r)
    }
    true
  }
}

/**
  * CheckpointCommitter that writes checkpoint ids to commit files.
  * The committer works with files in the local file system.
  *
  * NOTE: The base path must be accessible from all TaskManagers, so
  * - either a directory on a shared NFS or SAN that is mounted on the same path
  * - or on a local file system if the committer is only locally used.
  */
class FileCheckpointCommitter(val basePath: String) extends CheckpointCommitter {

  private var tempPath: String = _

  override def commitCheckpoint(subtaskIdx: Int, checkpointID: Long): Unit = {
    val commitPath = Paths.get(tempPath + "/" + subtaskIdx)

    // convert checkpointID to hexString
    val hexID = "0x" + StringUtils.leftPad(checkpointID.toHexString, 16, "0")
    // write hexString to commit file
    Files.write(commitPath, hexID.getBytes)
  }

  override def isCheckpointCommitted(subtaskIdx: Int, checkpointID: Long): Boolean = {
    val commitPath = Paths.get(tempPath + "/" + subtaskIdx)

    if (!Files.exists(commitPath)) {
      // no checkpoint has been committed if commit file does not exist
      false
    } else {
      // read committed checkpoint id from commit file
      val hexID = Files.readAllLines(commitPath).get(0)
      val checkpointed = java.lang.Long.decode(hexID)
      // check if committed id is less or equal to requested id
      checkpointID <= checkpointed
    }
  }

  override def createResource(): Unit = {
    this.tempPath = basePath + "/" + this.jobId

    // create directory for commit file
    Files.createDirectory(Paths.get(tempPath))
  }

  override def open(): Unit = {
    // no need to open a connection
  }

  override def close(): Unit = {
    // no need to close a connection
  }
}
