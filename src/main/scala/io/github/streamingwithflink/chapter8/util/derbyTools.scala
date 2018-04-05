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

import java.sql.DriverManager
import java.util.Properties

import scala.util.Random

/**
  * Methods to setup an embedded, in-memory Derby database.
  */
object DerbySetup {

  /** Sets up an embedded in-memory Derby database and creates the table. */
  def setupDerby(tableDDL: String): Unit = {
    // start embedded in-memory Derby and create a connection
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance()
    val props = new Properties()
    val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample;create=true", props)

    // create the table to which the sink writes
    val stmt = conn.createStatement()
    stmt.execute(tableDDL)

    stmt.close()
    conn.close()
  }

  /** Inserts initial data into a Derby table. */
  def initializeTable(stmt: String, params: Array[Array[Any]]): Unit = {
    // connect to embedded in-memory Derby and prepare query
    val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
    val prepStmt = conn.prepareStatement(stmt)

    for (stmtParams <- params) {
      for (i <- 1 to stmtParams.length) {
        prepStmt.setObject(i, stmtParams(i - 1))
      }
      // update the Derby table
      prepStmt.addBatch()
    }
    prepStmt.executeBatch()
  }
}

/**
  * A Runnable that queries the Derby table in intervals and prints the result.
  */
class DerbyReader(query: String, interval: Long) extends Runnable {

  // connect to embedded in-memory Derby and prepare query
  private val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
  private val prepStmt = conn.prepareStatement(query)
  private val numResultCols = prepStmt.getMetaData.getColumnCount

  override def run(): Unit = {
    val cols = new Array[Any](numResultCols)
    while(true) {
      // wait for the interval
      Thread.sleep(interval)
      // query the Derby table and print the result
      val res = prepStmt.executeQuery()
      while (res.next()) {
        for (i <- 1 to numResultCols) {
          cols(i - 1) = res.getObject(i)
        }
        println(s"${cols.mkString(", ")}")
      }
      res.close()
    }
  }
}

/**
  * A Runnable that writes in intervals to a Derby table.
  */
class DerbyWriter(stmt: String, paramGenerator: Random => Array[Any], interval: Long) extends Runnable {

  // connect to embedded in-memory Derby and prepare query
  private val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
  private val prepStmt = conn.prepareStatement(stmt)
  private val rand = new Random(1234)

  override def run(): Unit = {
    while(true) {
      Thread.sleep(interval)
      // get and set parameters
      val params = paramGenerator(rand)
      for (i <- 1 to params.length) {
        prepStmt.setObject(i, params(i - 1))
      }
      // update the Derby table
      prepStmt.executeUpdate()
    }
  }
}