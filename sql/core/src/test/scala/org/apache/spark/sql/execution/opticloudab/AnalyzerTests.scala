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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession

class AnalyzerTests extends SparkFunSuite
  with SharedSparkSession
  with Logging {

  test("UpdateFields test 00") {

    val ss: SparkSession = spark
    import ss.implicits._

    val df = sql("SELECT named_struct('a', 1, 'b', 2, 'b', 3) struct_col")
    val df1 = df
      .select($"struct_col"
        .withField("b", lit(100))
        .withField("c", lit(200))
      )

    df1.show()
    df1.explain(true)

  }


  test("Table-valued Functions (TVF) test 00") {

    val df = sql("SELECT explode(array(1, 3, 10))")

    df.show()
    df.explain(true)

  }

}
