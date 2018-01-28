/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.streamingwithflink.chapter5

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/** Object that defines the DataStream program in the main() method */
object KeyedTransformations {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // group sensor readings by their id
    val keyed: KeyedStream[SensorReading, String] = readings
      .keyBy(_.id)

    // a rolling reduce that computes the highest timestamp of the each group's readings
    val reducedSensors = keyed
      .reduce((r1, r2) => {
        val highestTimestamp = Math.max(r1.timestamp, r2.timestamp)
        SensorReading(r1.id, highestTimestamp, r1.temperature)
      })

    reducedSensors.print()

    // execute application
    env.execute("Keyed Transformations Example")
  }

}
