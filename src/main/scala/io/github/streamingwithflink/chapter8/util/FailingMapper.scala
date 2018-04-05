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

package io.github.streamingwithflink.chapter8.util

import org.apache.flink.api.common.functions.MapFunction

/**
  * An MapFunction that forwards all records.
  *
  * Any instance of the function will fail after forwarding a configured number of records by
  * throwing an exception.
  *
  * NOTE: This function is only used to demonstrate Flink's failure recovery capabilities.
  *
  * @param failInterval The number of records that are forwarded before throwing an exception.
  * @tparam IN The type of input and output records.
  */
class FailingMapper[IN](val failInterval: Int) extends MapFunction[IN, IN] {

  var cnt: Int = 0

  override def map(value: IN): IN = {

    cnt += 1
    // check failure condition
    if (cnt > failInterval) {
      throw new RuntimeException("Fail application to demonstrate output consistency.")
    }
    // forward value
    value
  }

}
