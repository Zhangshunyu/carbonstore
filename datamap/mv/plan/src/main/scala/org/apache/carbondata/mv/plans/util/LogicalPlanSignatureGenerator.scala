/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans.util

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
//import org.apache.spark.sql.catalyst.catalog._
import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.dsl._

object CheckSPJG {
  
  def isSPJG(subplan: LogicalPlan): Boolean = {
    subplan match {
      case a: Aggregate => {
        a.child.collect {
          case Join(_, _, _, _) | Project(_, _) | Filter(_, _) | HiveTableRelation(_,_,_) | LogicalRelation(_,_,_) | LocalRelation(_, _) => true
          case _ => false
        }.forall(identity)
      }
      case _ => false
    }
  }
}

object LogicalPlanSignatureGenerator extends SignatureGenerator[LogicalPlan] { 
  lazy val rule: SignatureRule[LogicalPlan] = LogicalPlanRule
  
  override def generate(plan: LogicalPlan): Option[Signature] = {
    if (plan.isSPJG) super.generate(plan)
    else None
  }
}

object LogicalPlanRule extends SignatureRule[LogicalPlan] {

  def apply(plan: LogicalPlan, childSignatures: Seq[Option[Signature]]): Option[Signature] = {
   
    plan match {
      case LogicalRelation(_,_,_) =>
        //TODO: implement this (link to BaseRelation)
        None
      case HiveTableRelation(tableMeta,_,_) =>
        Some(Signature(false, Set(Seq(tableMeta.database, tableMeta.identifier.table).mkString("."))))
      case l : LocalRelation =>
        // LocalRelation is for unit test cases
        Some(Signature(false, Set(l.toString())))
      case Filter(_, _) =>
        if (childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby) {
          // if (!childSignatures(0).getOrElse(Signature()).groupby) {
            childSignatures(0)
          // }
        } else {
          None
        }
      case Project(_, _) =>
        if (childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby) {
          childSignatures(0)
        } else {
          None
        }
      case Join(_, _, _, _) =>
        if (childSignatures.length == 2 &&
            !childSignatures(0).getOrElse(Signature()).groupby && 
            !childSignatures(1).getOrElse(Signature()).groupby) {
          Some(Signature(false, childSignatures(0).getOrElse(Signature()).datasets.union(childSignatures(1).getOrElse(Signature()).datasets)))
        } else {
          None
        }
      case Aggregate(_, _, _) =>
        if (childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby) {
            Some(Signature(true, childSignatures(0).getOrElse(Signature()).datasets))
        } else {
          None
        }
      case _ => None
    }
  }
}