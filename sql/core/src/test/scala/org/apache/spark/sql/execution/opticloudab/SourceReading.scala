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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSparkSession

class SourceReadingSuite
  extends SparkFunSuite
  with SharedSparkSession
  with Logging {
  import org.apache.spark.sql.SparkSession

  private def trace(df: DataFrame): Unit = {
    df.explain(true)
  }

  test("basic table") {
    val ss: SparkSession = spark
    import ss.implicits._

    val df = Seq(
      (1, true),
      (2, false)
    ).toDF("id", "flag")

    trace(df)

    val df1 = df.filter($"flag")
    trace(df1)
  }


}
