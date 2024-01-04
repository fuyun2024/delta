/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{RelationTimeTravel, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.REPARTITION_OPERATION

/**
 * Custom check rule that compensates for [SPARK-45383]. It checks the (unresolved) child relation
 * of each [[RelationTimeTravel]] in the plan, in order to trigger a helpful table-not-found
 * [[AnalysisException]] instead of the internal spark error that would otherwise result.
 */
class CheckUnresolvedRelationTimeTravel(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = {
    if (plan.containsPattern(REPARTITION_OPERATION)) {
      plan.foreachUp {
        case tt: RelationTimeTravel =>
          // Check if `tt.relation` is unresolved
          if (tt.relation.isInstanceOf[UnresolvedRelation]) {
            // You can choose your action here, e.g., logging a warning or throwing an exception
            val message = s"Found unresolved relation in time travel: ${tt.relation}"
            throw new AnalysisException(message)
          }
        case _ => ()
      }
    }
  }
}
