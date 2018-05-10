/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans.util

import org.apache.spark.sql.catalyst.trees.TreeNode
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
//import org.apache.spark.sql.types.DataType
//import org.apache.spark.sql.catalyst.plans.logical.Project
//import org.apache.spark.sql.catalyst.plans.logical.Aggregate
//import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
//import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.internal.Logging

case class Signature(groupby: Boolean = true, datasets: Set[String] = Set.empty)

abstract class SignatureRule[BaseType <: TreeNode[BaseType]] extends Logging {
  def apply(subplan: BaseType, signatures: Seq[Option[Signature]]): Option[Signature]
}

abstract class SignatureGenerator[BaseType <: TreeNode[BaseType]] extends Logging {
  protected val rule: SignatureRule[BaseType]

  def generate(subplan: BaseType): Option[Signature] = {
    generateUp(subplan)
  }
  
  protected def generateChildren(subplan: BaseType, nextOperation: BaseType => Option[Signature]): Seq[Option[Signature]] = {
    subplan.children.map { child =>
      nextOperation(child.asInstanceOf[BaseType])
    }
  }
  
  def generateUp(subplan: BaseType): Option[Signature] = {
    val childSignatures = generateChildren(subplan, t => generateUp(t))
    val lastSignature = rule(subplan, childSignatures)
    lastSignature
  }
}

