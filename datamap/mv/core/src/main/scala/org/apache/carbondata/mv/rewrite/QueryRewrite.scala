package org.apache.carbondata.mv.rewrite

import java.util.concurrent.atomic.AtomicLong
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.session.MVSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
/**
 * The primary workflow for rewriting relational queries using Spark libraries.  Designed to allow easy
 * access to the intermediate phases of query rewrite for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryRewrite private (
    mvSession: MVSession,
    logical: LogicalPlan,
    nextSubqueryId: AtomicLong) {
  self =>
  
  def this(mvSession: MVSession, logical: LogicalPlan) =
    this(mvSession, logical, new AtomicLong(0))
    
  def newSubsumerName(): String = s"gen_subsumer_${nextSubqueryId.getAndIncrement()}"

  lazy val optimizedPlan: LogicalPlan = mvSession.sessionState.optimizer.execute(logical)

  lazy val modularPlan: ModularPlan = mvSession.sessionState.modularizer.modularize(optimizedPlan).next().harmonized

  lazy val withSummaryData: ModularPlan = mvSession.sessionState.navigator.rewriteWithSummaryDatasets(modularPlan,self)

  lazy val toCompactSQL: String = withSummaryData.asCompactSQL
  
  lazy val toOneLineSQL: String = withSummaryData.asOneLineSQL
}