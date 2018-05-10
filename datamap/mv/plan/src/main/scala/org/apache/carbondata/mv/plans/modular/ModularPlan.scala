/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans.modular

import org.apache.carbondata.mv.plans._
import org.apache.spark.sql.catalyst.plans.QueryPlan
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
//import com.huawei.mqo.csetool.subexpressions.Signature
//import com.huawei.mqo.matching.Navigator
import org.apache.spark.sql.catalyst.expressions.Expression
//import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.JoinType
//import org.apache.spark.sql.catalyst.plans.LeftOuter
//import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{ HashMap, MultiMap }
import org.apache.carbondata.mv.plans.util._
import org.apache.carbondata.mv.plans.util.Printers
//import org.apache.carbondata.mv.plans.modular.Flags._
//import org.apache.carbondata.mv.plans.modular._
import scala.collection._
//import scala.util.{Try,Success,Failure}
import org.apache.spark.sql.catalyst.trees.TreeNode
//import org.apache.spark.sql.catalyst.expressions._
//import org.apache.spark.sql.catalyst.expressions.aggregate._

abstract class ModularPlan 
  extends QueryPlan[ModularPlan]
  with AggregatePushDown
  with Logging 
  with Serializable 
  with PredicateHelper with Printers {
  
  /**
   * the first two are to support sub-query expressions
   */  
  
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved
  
  def childrenResolved: Boolean = children.forall(_.resolved)
  
  private var statsCache: Option[Statistics] = None
  
  final def stats(spark: SparkSession, conf: SQLConf): Statistics = statsCache.getOrElse {
    statsCache = Some(computeStats(spark, conf))
    statsCache.get
  }
  
  final def invalidateStatsCache(): Unit = {
    statsCache = None
    children.foreach(_.invalidateStatsCache())
  }
  
  protected def computeStats(spark: SparkSession, conf: SQLConf): Statistics = {
//    spark.conf.set("spark.sql.cbo.enabled", true)
    val sqlStmt = asOneLineSQL
    val plan = spark.sql(sqlStmt).queryExecution.optimizedPlan
    plan.stats(conf)
  }
  
  override def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other)
  }

  private var _rewritten: Boolean = false

  /**
   * Marks this plan as already rewritten.
   */
  private[mv] def setRewritten(): ModularPlan = {
    _rewritten = true
    children.foreach(_.setRewritten())
    this
  }

  /**
   * Returns true if this node and its children have already been gone through query rewrite.
   * Note this this is only an optimization used to avoid rewriting trees that have already
   * been rewritten, and can be reset by transformations.
   */
  def rewritten: Boolean = _rewritten

  private var _skip: Boolean = false

  private[mv] def setSkip(): ModularPlan = {
    _skip = true
    children.foreach(_.setSkip())
    this
  }
  
  private[mv] def resetSkip(): ModularPlan = {
    _skip = false
    children.foreach(_.resetSkip())
    this
  }

  def skip: Boolean = _skip

  def isSPJGH: Boolean = this match {
    case modular.Select(_, _, _, _, _, Seq(modular.GroupBy(_, _, _, _, sel_c2 @ modular.Select(_, _, _, _, _, _, _,  _, _), _, _)), _, _, _) if sel_c2.children.forall(_.isInstanceOf[modular.LeafNode]) => true
    case modular.GroupBy(_, _, _, _, sel_c2 @ modular.Select(_, _, _, _, _, _, _, _, _), _, _) if (sel_c2.children.forall(_.isInstanceOf[modular.LeafNode])) => true
    case modular.Select(_, _, _, _, _, children, _, _, _) if (children.forall(_.isInstanceOf[modular.LeafNode])) => true
    case _ => false
  }

  def signature: Option[Signature] = ModularPlanSignatureGenerator.generate(this)

  def createMutableAdjacencyList(edges: Seq[JoinEdge]) = {
    val mm = new HashMap[Int, mutable.Set[(Int, JoinType)]] with MultiMap[Int, (Int, JoinType)]
    for (edge <- edges) { mm.addBinding(edge.left, (edge.right, edge.joinType)) }
    mm
  }
  
  def createImmutableAdjacencyList(edges: Seq[JoinEdge]) = {
    edges.groupBy{_.left}.map { case (k,v) => (k, v.map(e => (e.right,e.joinType)))}
  }

  def adjacencyList: Map[Int, Seq[(Int, JoinType)]] = Map.empty

  def extractJoinConditions(left: ModularPlan, right: ModularPlan) = Seq.empty[Expression]

  def extractRightEvaluableConditions(left: ModularPlan, right: ModularPlan) = Seq.empty[Expression]

  def extractEvaluableConditions(plan: ModularPlan) = Seq.empty[Expression]
  
  def asCompactSQL: String = asCompactString(new SQLBuilder(this).fragmentExtract)

  def asOneLineSQL: String = asOneLineString(new SQLBuilder(this).fragmentExtract)
  
  // for plan without sub-query expression only
  def asOneLineSQL(subqueryPrefix: String): String = asOneLineString(new SQLBuilder(this, subqueryPrefix).fragmentExtract)

  /**
   * Returns a plan where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but replaces harmonized dimension table with HarmonizedRelation
   * and fact table with sub-plan that pre-aggregates the table before join with dimension table
   *
   * Some nodes should overwrite this to provide proper harmonization logic.
   */
  lazy val harmonized: ModularPlan = DefaultHarmonizer.execute(preHarmonized)
  
  /**
   * Do some simple transformation on this plan before harmonizing. Implementations can override
   * this method to provide customized harmonize logic without rewriting the whole logic.
   * 
   * We assume queries need to be harmonized are of the form:
   * 
   *  FACT (left) join (harmonized) DIM1 (left) join (harmonized) DIM2 ...
   *  
   * For queries of not this form, customize this method for them to conform this form.   
   */
  protected def preHarmonized: ModularPlan = this
}

object ModularPlan extends PredicateHelper {

}

abstract class LeafNode extends ModularPlan {
  override def children: Seq[ModularPlan] = Nil
}

abstract class UnaryNode extends ModularPlan {
  def child: ModularPlan

  override def children: Seq[ModularPlan] = child :: Nil
}

abstract class BinaryNode extends ModularPlan {
  def left: ModularPlan
  def right: ModularPlan

  override def children: Seq[ModularPlan] = Seq(left, right)
}
