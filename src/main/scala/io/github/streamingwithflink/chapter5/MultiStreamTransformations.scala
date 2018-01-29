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

import io.github.streamingwithflink.chapter5.util.{SmokeLevel, SmokeLevelSource}
import io.github.streamingwithflink.chapter5.util.SmokeLevel.SmokeLevel
import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** Object that defines the DataStream program in the main() method */
object MultiStreamTransformations {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val tempReadings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // ingest smoke level stream
    val smokeReadings: DataStream[SmokeLevel] = env
      .addSource(new SmokeLevelSource)
      .setParallelism(1)

    // group sensor readings by their id
    val keyed: KeyedStream[SensorReading, String] = tempReadings
      .keyBy(_.id)

    val alerts = keyed.connect(smokeReadings.broadcast).flatMap(new RaiseAlertFlatMap)

    alerts.print()

    // execute application
    env.execute("Multi-Stream Transformations Example")
  }

  /**
    *
    */
  class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading, SmokeLevel, SensorReading] {

    var smokeLevel = SmokeLevel.Low

    override def flatMap1(in1: SensorReading, collector: Collector[SensorReading]): Unit = {
      // high chance of fire => true
      if (smokeLevel.equals(SmokeLevel.High) && in1.temperature > 100) {
        collector.collect(in1)
      }
    }

    override def flatMap2(in2: SmokeLevel, collector: Collector[SensorReading]): Unit = {
      smokeLevel = in2
    }
  }
}
