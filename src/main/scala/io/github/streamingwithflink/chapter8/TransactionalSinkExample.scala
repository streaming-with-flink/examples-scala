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

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset}

import io.github.streamingwithflink.chapter8.util.FailingMapper
import io.github.streamingwithflink.util.{ResettableSensorSource, SensorReading, SensorTimeAssigner}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Example program that demonstrates the behavior of a 2-phase-commit (2PC) sink that writes
  * output to files.
  *
  * The 2PC sink guarantees exactly-once output by writing records immediately to a
  * temp file. For each checkpoint, a new temp file is created. When a checkpoint
  * completes, the corresponding temp file is committed by moving it to a target directory.
  *
  * The program includes a MapFunction that throws an exception in regular intervals to simulate
  * application failures.
  * You can compare the behavior of a 2PC sink and the regular print sink in failure cases.
  *
  * - The TransactionalFileSink commits a file to the target directory when a checkpoint completes
  * and prevents duplicated result output.
  * - The regular print() sink writes to the standard output when a result is produced and
  * duplicates result output in case of a failure.
  *
  */
object TransactionSinkExample {

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
    // write to files with a transactional sink.
    // results are committed when a checkpoint is completed.
    val (targetDir, transactionDir) = createAndGetPaths
    avgTemp.addSink(new TransactionalFileSink(targetDir, transactionDir))

    // OPTION 2 (uncomment to enable)
    // --------
    // print to standard out without write-ahead log.
    // results are printed as they are produced and re-emitted in case of a failure.
//    avgTemp.print()
//      // enforce sequential writing
//      .setParallelism(1)

    env.execute()
  }

  /** Creates temporary paths for the output of the transactional file sink. */
  def createAndGetPaths: (String, String) = {
    val tempDir = System.getProperty("java.io.tmpdir")
    val targetDir = s"$tempDir/committed"
    val transactionDir = s"$tempDir/transaction"

    val targetPath = Paths.get(targetDir)
    val transactionPath = Paths.get(transactionDir)

    if (!Files.exists(targetPath)) {
      Files.createDirectory(targetPath)
    }
    if (!Files.exists(transactionPath)) {
      Files.createDirectory(transactionPath)
    }

    (targetDir, transactionDir)
  }
}

/**
  * Transactional sink that writes records to files an commits them to a target directory.
  *
  * Records are written as they are received into a temporary file. For each checkpoint, there is
  * a dedicated file that is committed once the checkpoint (or a later checkpoint) completes.
  */
class TransactionalFileSink(val targetPath: String, val tempPath: String)
    extends TwoPhaseCommitSinkFunction[(String, Double), String, Void](
      createTypeInformation[String].createSerializer(new ExecutionConfig),
      createTypeInformation[Void].createSerializer(new ExecutionConfig)) {

  var transactionWriter: BufferedWriter = _

  /**
    * Creates a temporary file for a transaction into which the records are
    * written.
    */
  override def beginTransaction(): String = {

    // path of transaction file is constructed from current time and task index
    val timeNow = LocalDateTime.now(ZoneId.of("UTC"))
      .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    val transactionFile = s"$timeNow-$taskIdx"

    // create transaction file and writer
    val tFilePath = Paths.get(s"$tempPath/$transactionFile")
    Files.createFile(tFilePath)
    this.transactionWriter = Files.newBufferedWriter(tFilePath)
    println(s"Creating Transaction File: $tFilePath")

    // name of transaction file is returned to later identify the transaction
    transactionFile
  }

  /** Write record into the current transaction file. */
  override def invoke(transaction: String, value: (String, Double), context: Context[_]): Unit = {
    transactionWriter.write(value.toString)
    transactionWriter.write('\n')
  }

  /** Flush and close the current transaction file. */
  override def preCommit(transaction: String): Unit = {
    transactionWriter.flush()
    transactionWriter.close()
  }

  /** Commit a transaction by moving the pre-committed transaction file
    * to the target directory.
    */
  override def commit(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    // check if the file exists to ensure that the commit is idempotent.
    if (Files.exists(tFilePath)) {
      val cFilePath = Paths.get(s"$targetPath/$transaction")
      Files.move(tFilePath, cFilePath)
    }
  }

  /** Aborts a transaction by deleting the transaction file. */
  override def abort(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    if (Files.exists(tFilePath)) {
      Files.delete(tFilePath)
    }
  }
}