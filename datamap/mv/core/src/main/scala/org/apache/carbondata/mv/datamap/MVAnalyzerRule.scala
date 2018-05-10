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
package org.apache.carbondata.mv.datamap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, ScalaUDF}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Command, DeserializeToObject, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.QueryExecution

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.mv.rewrite.{SummaryDataset, SummaryDatasetCatalog}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.mv.session.MVSession
import org.apache.carbondata.mv.session.internal.SharedState
import org.apache.carbondata.mv.rewrite._

/**
 * Analyzer rule to rewrite the query for MV datamap
 *
 * @param sparkSession
 */
class MVAnalyzerRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private val dataMapProvider =
    DataMapManager.get().getDataMapProvider(DataMapClassProvider.MV.getShortName, sparkSession)

  private val LOGGER = LogServiceFactory.getLogService(classOf[MVAnalyzerRule].getName)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    var needAnalysis = true
    plan.transformAllExpressions {
      // first check if any preAgg scala function is applied it is present is in plan
      // then call is from create preaggregate table class so no need to transform the query plan
      // TODO Add different UDF name
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase("preAgg") =>
        needAnalysis = false
        al
      // in case of query if any unresolve alias is present then wait for plan to be resolved
      // return the same plan as we can tranform the plan only when everything is resolved
      case unresolveAlias@UnresolvedAlias(_, _) =>
        needAnalysis = false
        unresolveAlias
      case attr@UnresolvedAttribute(_) =>
        needAnalysis = false
        attr
    }
    val catalog = DataMapStoreManager.getInstance().getDataMapCatalog(dataMapProvider,
      DataMapClassProvider.MV.getShortName).asInstanceOf[SummaryDatasetCatalog]
    if (needAnalysis && catalog != null && isValidPlan(plan, catalog)) {
      val mvSession = new MVSession(sparkSession,catalog)
      val compactSQL = mvSession.rewriteToSQL(plan)
      if (compactSQL.equals("")) plan
      else {
        // TODO: this is hack.  need an analyzer without this rule
        val parser = new CarbonSpark2SqlParser
        val updatedQuery = parser.addPreAggFunction(compactSQL)
//        val analyzed = sparkSession.sql(updatedQuery).drop("preAgg").queryExecution.analyzed
        val test1 = sparkSession.sql(updatedQuery).queryExecution.analyzed
        val analyzed = sparkSession.sql(updatedQuery).queryExecution.analyzed.
                       transform { case p @ logical.Project(head::tail, _) if head.isInstanceOf[Alias] && head.name.equals("preAgg")=> p.copy(projectList=tail)
                                   case a @ logical.Aggregate(_,head::tail,_) if head.isInstanceOf[Alias] && head.name.equals("preAgg")=> a.copy(aggregateExpressions=tail)}
//        val planWithCache = sparkSession.sharedState.cacheManager.useCachedData(analyzed)
        val planWithMV = catalog.useSummaryDataset(analyzed)
        planWithMV
      }
    } else plan
//      val modularPlan = catalog.mVState.rewritePlan(plan).withSummaryData
//      if (modularPlan.find (_.rewritten).isDefined) {
//        val compactSQL = modularPlan.asCompactSQL
//        LOGGER.audit(s"\n$compactSQL\n")
//        val analyzed = sparkSession.sql(compactSQL).queryExecution.analyzed
//        analyzed
//      } else {
//        plan
//      }
//    } else {
//      plan
//    }
  }

  def isValidPlan(plan: LogicalPlan, catalog: SummaryDatasetCatalog): Boolean = {
    !plan.isInstanceOf[Command] && !isDataMapExists(plan, catalog.listAllSchema()) &&
    !plan.isInstanceOf[DeserializeToObject]
  }
  /**
   * Check whether datamap table already updated in the query.
   *
   * @param plan
   * @param mvs
   * @return
   */
  def isDataMapExists(plan: LogicalPlan, mvs: Array[SummaryDataset]): Boolean = {
    val catalogs = plan collect {
      case l: LogicalRelation => l.catalogTable
    }
    catalogs.isEmpty || catalogs.exists { c =>
      mvs.exists { mv =>
        val identifier = mv.dataMapSchema.getRelationIdentifier
        identifier.getTableName.equals(c.get.identifier.table) &&
        identifier.getDatabaseName.equals(c.get.database)
      }
    }
  }
}
