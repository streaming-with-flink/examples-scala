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
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** Object that defines the DataStream program in the main() method */
object BasicTransformations {

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

    // filter out sensor measurements from sensors with temperature under 25 degrees
    val filteredSensors: DataStream[SensorReading] = readings
      .filter( r =>  r.temperature >= 25)

   // the above filter transformation using a UDF
   // val filteredSensors: DataStream[SensorReading] = readings
   //   .filter(new TemperatureFilter(25))

    // project the id of each sensor reading
    val sensorIds: DataStream[String] = filteredSensors
      .map( r => r.id )

    // the above map transformation using a UDF
    // val sensorIds2: DataStream[String] = readings
    //   .map(new ProjectionMap)

    // split the String id of each sensor to the prefix "sensor" and sensor number
    val splitIds: DataStream[String] = sensorIds
      .flatMap( id => id.split("_") )

    // the above flatMap transformation using a UDF
    // val splitIds: DataStream[String] = sensorIds
    //  .flatMap( new SplitIdFlatMap )

    // print result stream to standard out
    splitIds.print()

    // execute application
    env.execute("Basic Transformations Example")
  }

  /** User-defined FilterFunction to filter out SensorReading with temperature below the threshold */
  class TemperatureFilter(threshold: Long) extends FilterFunction[SensorReading] {

    override def filter(r: SensorReading): Boolean = r.temperature >= threshold

  }

  /** User-defined MapFunction to project a sensor's id */
  class ProjectionMap extends MapFunction[SensorReading, String] {

    override def map(r: SensorReading): String  = r.id

  }

  /** User-defined FlatMapFunction that splits a sensor's id String into a prefix and a number */
  class SplitIdFlatMap extends FlatMapFunction[String, String] {

    override def flatMap(id: String, collector: Collector[String]): Unit = id.split("_")

  }

}
