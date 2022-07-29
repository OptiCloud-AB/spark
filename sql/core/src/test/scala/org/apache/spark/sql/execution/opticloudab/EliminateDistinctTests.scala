/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.opticloudab

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

class EliminateDistinctTests extends SparkFunSuite
  with SharedSparkSession
  with Logging {


  test("basic") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = Seq(
      (1, true),
      (2, false),
      (4, false)
    ).toDF("id", "flag")

    df.createOrReplaceTempView("table")

    val df1 = ss.sql("SELECT DISTINCT(MAX(id)) FROM table")
    df1.show(false)

    df1.explain(true)
  }

  test("basic unions") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = Seq(
      (1, true),
      (2, false),
      (4, false)
    ).toDF("id", "flag")

    val df1 = df.union(df.union(df.union(df)).union(df))
    df1.show(false)

    df1.explain(true)
  }

  test("basic ConvertToLocalRelation") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = Seq(
      (1, true),
      (2, false),
      (4, false)
    ).toDF("id", "flag")

    df.createOrReplaceTempView("table")

    val df1 = ss.sql("SELECT id FROM table WHERE flag = true")
    df1.show(false)

    df1.explain(true)
  }
}
