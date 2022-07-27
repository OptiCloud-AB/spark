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
import org.apache.spark.sql.catalyst.expressions.{Add, GreaterThan, Literal, Subtract}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.test.SharedSparkSession

object SimpleFilterFolding extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case filter @ Filter(_, _) => filter transformExpressionsUp {
        case GreaterThan(Add(left, literal: Literal, _), right) =>
          GreaterThan(left, Subtract(right, literal))
      }
    }
  }
}

object SimpleOptimizer extends RuleExecutor[LogicalPlan] {
  val batches = Batch("Filter folding", Once,
//    SimpleFilterFolding) :: Nil
    SimpleFilterFolding, ConstantFolding) :: Nil
}

class SimpleFilterFoldingTests  extends SparkFunSuite
  with SharedSparkSession
  with Logging {

  import org.apache.spark.sql.catalyst.expressions._
  import org.apache.spark.sql.catalyst.plans.logical._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  import org.slf4j.Logger
  import org.apache.spark.sql.execution.opticloudab.TestTracer._
  implicit val _log: Logger = log

  test("filter folding basic") {
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
    val optimizedPlan = SimpleOptimizer.execute(analyzedPlan)
    tracePlan("optimizedPlan", optimizedPlan)
  }
}
