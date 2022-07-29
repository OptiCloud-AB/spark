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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession

class CustomOptimizerFilterPushDown extends SparkFunSuite
  with SharedSparkSession
  with Logging {
  // see https://databricks-prod-cloudfront.cloud.databricks.com/\
  //  public/4027ec902e239c93eaaa8714f173bcfc/3741049972324885/\
  //  4201913720573284/4413065072037724/latest.html

  private def trace(df: DataFrame): Unit = {
    df.explain(true)
  }

  val items = Seq(
    (0, "Macbook Pro", 1999.0),
    (1, "Macbook Air", 1500.0),
    (2, "iPad Air", 1200.0)
  )

  val orders = Seq(
    (100, 0, 1),
    (100, 1, 1),
    (101, 2, 3)
  )

  test("more complex table") {
    val ss: SparkSession = spark
    import ss.implicits._

    val itemsDf = items.toDF("id", "name", "price")
    val ordersDf = orders.toDF("id", "itemid", "count")

    itemsDf.createOrReplaceTempView("item")
    ordersDf.createOrReplaceTempView("order")

    val query =
      """
        | SELECT order.id, item.name, item.price, order.count
        | FROM item
        | JOIN order
        | WHERE item.id = order.itemid
        |""".stripMargin
    val q = sql(query)
    q.show()
    trace(q)

    val query2 =
      """
        | SELECT order.id, item.name, item.price, order.count
        | FROM item
        | JOIN order
        | WHERE item.id = order.itemid AND item.price < 1400 AND order.count > 2 - 1
        |""".stripMargin
    val q2 = sql(query2)
    q2.show()
    trace(q2)
  }

  import org.apache.spark.sql.catalyst.expressions._
  import org.apache.spark.sql.catalyst.plans.logical._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._
  import org.apache.spark.sql.catalyst.optimizer._

  import org.slf4j.Logger
  import org.apache.spark.sql.execution.opticloudab.TestTracer._
  implicit val _log: Logger = log

  test("Write rules for logical plan") {
    val add = Add(Literal(2), Literal(3))
    TestTracer.trace("add", add.numberedTreeString)

    val sub = add transform {
      case Add(l, r, _) => Subtract(l, r)
    }
    TestTracer.trace("sub", sub.treeString)
  }

  test("4. Existing filter optimizations") {
    val logicalPlan = LocalRelation('a.int, 'b.string)
      .select($"a")
      .where(
        GreaterThan(
          Add('a, Literal(1)),
          Literal(2)
        )
      )
    tracePlan("logicalPlan", logicalPlan)
    val analyzedPlan = logicalPlan.analyze
    tracePlan("analyzedPlan", analyzedPlan)
    val optimizedPlan = SimpleTestOptimizer.execute(analyzedPlan)
    tracePlan("optimizedPlan", optimizedPlan)
  }
}
