/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans.modular

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans._

private[mv] trait Matchable extends ModularPlan {
  def outputList: Seq[NamedExpression]
  def predicateList: Seq[Expression]
}

///**
// * :: DeveloperApi ::
// */
//@DeveloperApi
case class GroupBy(outputList: Seq[NamedExpression], inputList: Seq[Expression], predicateList: Seq[Expression], alias: Option[String], child: ModularPlan, flags: FlagSet, flagSpec: Seq[Seq[Any]]) extends UnaryNode with Matchable {
  override def output: Seq[Attribute] = outputList.map(_.toAttribute)
}

///**
// * :: DeveloperApi ::
// */
//@DeveloperApi
case class Select(outputList: Seq[NamedExpression], inputList: Seq[Expression], predicateList: Seq[Expression], aliasMap: Map[Int, String], joinEdges: Seq[JoinEdge], children: Seq[ModularPlan], flags: FlagSet, flagSpec: Seq[Seq[Any]], windowSpec: Seq[Seq[Any]]) extends ModularPlan with Matchable {
  override def output: Seq[Attribute] = outputList.map(_.toAttribute)

  override def adjacencyList: scala.collection.immutable.Map[Int, Seq[(Int, JoinType)]] =
    joinEdges.groupBy { _.left }.map { case (k, v) => (k, v.map(e => (e.right, e.joinType))) }

  override def extractJoinConditions(left: ModularPlan, right: ModularPlan) = {
    predicateList.filter(p => p.references.intersect(left.outputSet).nonEmpty &&
      p.references.intersect(right.outputSet).nonEmpty &&
      p.references.subsetOf(left.outputSet ++ right.outputSet))
  }

  override def extractRightEvaluableConditions(left: ModularPlan, right: ModularPlan) = {
    predicateList.filter(p => p.references.subsetOf(left.outputSet ++ right.outputSet) &&
      p.references.intersect(right.outputSet).nonEmpty)
  }

  override def extractEvaluableConditions(plan: ModularPlan) = {
    predicateList.filter(p => canEvaluate(p, plan))
  }
}

///**
// * :: DeveloperApi ::
// */
//@DeveloperApi
case class Union(children: Seq[ModularPlan], flags: FlagSet, flagSpec: Seq[Seq[Any]]) extends ModularPlan {
  override def output: Seq[Attribute] = children.head.output
}

case object OneRowTable extends LeafNode {
  override def output: Seq[Attribute] = Nil
}




