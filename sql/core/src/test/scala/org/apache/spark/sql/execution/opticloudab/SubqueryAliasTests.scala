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

class SubqueryAliasTests extends SparkFunSuite
  with SharedSparkSession
  with Logging {

  import TestTracer._

  import org.slf4j.Logger
  implicit val _log: Logger = log

  test("basic") {

    import org.apache.spark.sql.catalyst.dsl.plans._

    val t1 = table("t1")

    val plan = t1.subquery('a)

    tracePlan("logical: ", plan)

  }


  test("basic table") {

    val ss: SparkSession = spark
    import ss.implicits._

    val items = Seq(
      (0, "Macbook Pro", 1999.0),
      (1, "Macbook Air", 1500.0),
      (2, "iPad Air", 1200.0)
    ).toDF("id", "name", "price")
    items.createOrReplaceTempView("item")

    val query =
      """
        | SELECT item.name, item.price
        | FROM item
        |""".stripMargin
    val q = sql(query)
    q.show()
    q.explain(true)
  }

}
