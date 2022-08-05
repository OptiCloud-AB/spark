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

package org.apache.spark.sql.execution.opticloudab.parser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedSubqueryColumnAliases
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnresolvedWith}

class ParserTests extends PlanTest with Logging {
  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._
  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan, checkAnalysis = false)
  }

  test("comments") {
    // def table(parts: String*): LogicalPlan = UnresolvedRelation(parts)
    val plan = table("a").select(star())
    log.warn("\n" + plan.treeString)
    val sql1 =
      """
        |-- single comment \
        |with line continuity
        |SELECT * FROM a
        |""".stripMargin
    assertEqual(sql1, plan)

    val sql2 =
      """
        | /*
        |  * bla-bla
        |  * select * from b
        |  */
        |SELECT * FROM a
        |""".stripMargin
    assertEqual(sql2, plan)
  }

  private def cte(
                   plan: LogicalPlan,
                   namedPlans: (String, (LogicalPlan, Seq[String]))*): UnresolvedWith = {
    val ctes = namedPlans.map {
      case (name, (cte, columnAliases)) =>
        val subquery = if (columnAliases.isEmpty) {
          cte
        } else {
          UnresolvedSubqueryColumnAliases(columnAliases, cte)
        }
        name -> SubqueryAlias(name, subquery)
    }
    UnresolvedWith(plan, ctes)
  }

  test("cte") {
    val cte1 = cte(
      table("cte1").select(star()),
      "cte1" -> (
        (table("a").select(star()), Seq.empty))
    )
    log.warn("\n" + cte1.treeString)
    val sql1 =
      """
        |WITH cte1 AS (SELECT * from a)
        | SELECT * FROM cte1
        |""".stripMargin

    assertEqual(sql1, cte1)
  }
}
