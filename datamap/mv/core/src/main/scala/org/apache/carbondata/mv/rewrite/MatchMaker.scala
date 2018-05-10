/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.catalyst.trees.TreeNode
//import org.apache.spark.sql.catalyst.expressions.AttributeReference
//import org.apache.spark.sql.catalyst.expressions.Alias
//import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.internal.Logging
//import org.apache.carbondata.mv.plans.modular
//import org.apache.carbondata.mv.plans.modular.ModularPlan
//import org.apache.carbondata.mv.plans.util.SQLBuilder

abstract class MatchPattern[MatchingPlan <: TreeNode[MatchingPlan]] extends Logging {
  
  def apply(subsumer: MatchingPlan,subsumee: MatchingPlan,compensation: Option[MatchingPlan],rewrite: QueryRewrite): Seq[MatchingPlan]
  
}

abstract class MatchMaker[MatchingPlan <: TreeNode[MatchingPlan]] {

  /** Define a sequence of rules, to be overridden by the implementation. */ 
  protected val patterns: Seq[MatchPattern[MatchingPlan]]
  
  def execute(subsumer: MatchingPlan,subsumee: MatchingPlan,compensation: Option[MatchingPlan],rewrite:QueryRewrite): Iterator[MatchingPlan] = {
    val iter = patterns.view.flatMap(_(subsumer, subsumee, compensation, rewrite)).toIterator
    iter

  }
}