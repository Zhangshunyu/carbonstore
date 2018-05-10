

package org.apache.carbondata.mv.session.internal

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.AnalyzeTableCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryManager}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.carbondata.mv.plans.util.BirdcageOptimizer
import org.apache.carbondata.mv.session.MVSession
import org.apache.carbondata.mv.rewrite.QueryRewrite
import org.apache.carbondata.mv.plans.modular.SimpleModularizer
import org.apache.carbondata.mv.rewrite.DefaultMatchMaker
import org.apache.carbondata.mv.rewrite.Navigator
import org.apache.carbondata.mv.rewrite.SummaryDatasetCatalog

/**
 * A class that holds all session-specific state in a given [[MVSession]].
 */
private[mv] class SessionState(mvSession: MVSession) {

  // Note: These are all lazy vals because they depend on each other (e.g. conf) and we
  // want subclasses to override some of the fields. Otherwise, we would get a lot of NPEs.

  /**
   * Internal catalog for managing table and database states.
   */
  lazy val catalog = mvSession.sharedState.summaryDatasetCatalog
    
//  lazy val catalog = new SessionCatalog(
//    mqoSession.sharedState.externalCatalog,
//    mqoSession.sharedState.globalTempViewManager,
//    functionResourceLoader,
//    functionRegistry,
//    conf,
//    newHadoopConf())
  
  /**
   * Modular query plan modularizer
   */
  lazy val modularizer = SimpleModularizer

  /**
   * Logical query plan optimizer.
   */
  lazy val optimizer = BirdcageOptimizer
  
  lazy val matcher = DefaultMatchMaker
  
  lazy val navigator: Navigator = new Navigator(catalog, mvSession)


  def rewritePlan(plan: LogicalPlan): QueryRewrite = new QueryRewrite(mvSession, plan)

}
