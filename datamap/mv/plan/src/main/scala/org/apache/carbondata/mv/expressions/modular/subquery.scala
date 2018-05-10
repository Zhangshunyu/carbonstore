/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.expressions.modular

import org.apache.spark.sql.catalyst.expressions.PlanExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.AttributeSeq
import org.apache.spark.sql.catalyst.expressions.OuterReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Unevaluable
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.Predicate
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.types._
import org.apache.carbondata.mv.plans.modular.ModularPlan

/**
 * A base interface for expressions that contain a [[ModularPlan]].
 */
abstract class ModularSubquery(
    plan: ModularPlan,
    children: Seq[Expression],
    exprId: ExprId) extends PlanExpression[ModularPlan] {
  override lazy val resolved: Boolean = childrenResolved && plan.resolved
  override lazy val references: AttributeSet = 
    if (plan.resolved) super.references -- plan.outputSet else super.references
  override def withNewPlan(plan: ModularPlan): ModularSubquery
  override def semanticEquals(o: Expression): Boolean = o match {
    case p: ModularSubquery =>
      this.getClass.getName.equals(p.getClass.getName) && plan.sameResult(p.plan) &&
        children.length == p.children.length &&
        children.zip(p.children).forall(p => p._1.semanticEquals(p._2))
    case _ => false
  }
  def canonicalize(attrs: AttributeSeq): ModularSubquery = {
    // Normalize the outer references in the subquery plan.
    val normalizedPlan = plan.transformAllExpressions {
      case OuterReference(r) => OuterReference(QueryPlan.normalizeExprId(r, attrs))
    }
    withNewPlan(normalizedPlan).canonicalized.asInstanceOf[ModularSubquery]
  }
}

//object ModularSubquery {
//  /**
//   * Returns true when an expression contains an IN or EXISTS subquery and false otherwise.
//   */
//  def hasInOrExistsSubquery(e: Expression): Boolean = {
//    e.find {
//      case _: ListQuery | _: Exists => true
//      case _ => false
//    }.isDefined
//  }

//  /**
//   * Returns true when an expression contains a subquery that has outer reference(s). The outer
//   * reference attributes are kept as children of subquery expression by
//   * [[org.apache.spark.sql.catalyst.analysis.Analyzer.ResolveSubquery]]
//   */
//  def hasCorrelatedSubquery(e: Expression): Boolean = {
//    e.find {
//      case s: ModularSubquery => s.children.nonEmpty
//      case _ => false
//    }.isDefined
//  }
//}

//object SubExprUtils extends PredicateHelper {
//  /**
//   * Returns true when an expression contains correlated predicates i.e outer references and
//   * returns false otherwise.
//   */
//  def containsOuter(e: Expression): Boolean = {
//    e.find(_.isInstanceOf[OuterReference]).isDefined
//  }
//
//  /**
//   * Returns whether there are any null-aware predicate subqueries inside Not. If not, we could
//   * turn the null-aware predicate into not-null-aware predicate.
//   */
//  def hasNullAwarePredicateWithinNot(condition: Expression): Boolean = {
//    splitConjunctivePredicates(condition).exists {
//      case _: Exists | Not(_: Exists) => false
//      case In(_, Seq(_: ListQuery)) | Not(In(_, Seq(_: ListQuery))) => false
//      case e => e.find { x =>
//        x.isInstanceOf[Not] && e.find {
//          case In(_, Seq(_: ListQuery)) => true
//          case _ => false
//        }.isDefined
//      }.isDefined
//    }
//
//  }

//  /**
//   * Returns an expression after removing the OuterReference shell.
//   */
//  def stripOuterReference(e: Expression): Expression = e.transform { case OuterReference(r) => r }
//
//  /**
//   * Returns the list of expressions after removing the OuterReference shell from each of
//   * the expression.
//   */
//  def stripOuterReferences(e: Seq[Expression]): Seq[Expression] = e.map(stripOuterReference)
//
//  /**
//   * Returns the logical plan after removing the OuterReference shell from all the expressions
//   * of the input logical plan.
//   */
//  def stripOuterReferences(p: ModularPlan): ModularPlan = {
//    p.transformAllExpressions {
//      case OuterReference(a) => a
//    }
//  }

//  /**
//   * Given a logical plan, returns TRUE if it has an outer reference and false otherwise.
//   */
//  def hasOuterReferences(plan: ModularPlan): Boolean = {
//    plan.find {
//      case f: Select => containsOuter(f.condition)
//      case other => false
//    }.isDefined
//  }

//  /**
//   * Given a list of expressions, returns the expressions which have outer references. Aggregate
//   * expressions are treated in a special way. If the children of aggregate expression contains an
//   * outer reference, then the entire aggregate expression is marked as an outer reference.
//   * Example (SQL):
//   * {{{
//   *   SELECT a FROM l GROUP by 1 HAVING EXISTS (SELECT 1 FROM r WHERE d < min(b))
//   * }}}
//   * In the above case, we want to mark the entire min(b) as an outer reference
//   * OuterReference(min(b)) instead of min(OuterReference(b)).
//   * TODO: Currently we don't allow deep correlation. Also, we don't allow mixing of
//   * outer references and local references under an aggregate expression.
//   * For example (SQL):
//   * {{{
//   *   SELECT .. FROM p1
//   *   WHERE EXISTS (SELECT ...
//   *                 FROM p2
//   *                 WHERE EXISTS (SELECT ...
//   *                               FROM sq
//   *                               WHERE min(p1.a + p2.b) = sq.c))
//   *
//   *   SELECT .. FROM p1
//   *   WHERE EXISTS (SELECT ...
//   *                 FROM p2
//   *                 WHERE EXISTS (SELECT ...
//   *                               FROM sq
//   *                               WHERE min(p1.a) + max(p2.b) = sq.c))
//   *
//   *   SELECT .. FROM p1
//   *   WHERE EXISTS (SELECT ...
//   *                 FROM p2
//   *                 WHERE EXISTS (SELECT ...
//   *                               FROM sq
//   *                               WHERE min(p1.a + sq.c) > 1))
//   * }}}
//   * The code below needs to change when we support the above cases.
//   */
//  def getOuterReferences(conditions: Seq[Expression]): Seq[Expression] = {
//    val outerExpressions = ArrayBuffer.empty[Expression]
//    conditions foreach { expr =>
//      expr transformDown {
//        case a: AggregateExpression if a.collectLeaves.forall(_.isInstanceOf[OuterReference]) =>
//          val newExpr = stripOuterReference(a)
//          outerExpressions += newExpr
//          newExpr
//        case OuterReference(e) =>
//          outerExpressions += e
//          e
//      }
//    }
//    outerExpressions
//  }

//  /**
//   * Returns all the expressions that have outer references from a logical plan. Currently only
//   * Filter operator can host outer references.
//   */
//  def getOuterReferences(plan: ModularPlan): Seq[Expression] = {
//    val conditions = plan.collect { case Filter(cond, _) => cond }
//    getOuterReferences(conditions)
//  }
//
//  /**
//   * Returns the correlated predicates from a logical plan. The OuterReference wrapper
//   * is removed before returning the predicate to the caller.
//   */
//  def getCorrelatedPredicates(plan: ModularPlan): Seq[Expression] = {
//    val conditions = plan.collect { case Filter(cond, _) => cond }
//    conditions.flatMap { e =>
//      val (correlated, _) = splitConjunctivePredicates(e).partition(containsOuter)
//      stripOuterReferences(correlated) match {
//        case Nil => None
//        case xs => xs
//      }
//    }
//  }
//}

/**
 * A subquery that will return only one row and one column. This will be converted into a physical
 * scalar subquery during planning.
 *
 * Note: `exprId` is used to have a unique name in explain string output.
 */
case class ScalarModularSubquery(
    plan: ModularPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends ModularSubquery(plan, children, exprId) with Unevaluable {
  override def dataType: DataType = plan.schema.fields.head.dataType
  override def nullable: Boolean = true
  override def withNewPlan(plan: ModularPlan): ScalarModularSubquery = copy(plan = plan)
  override def toString: String = s"scalar-modular-subquery#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ScalarModularSubquery(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}

object ScalarModularSubquery {
  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.find {
      case s: ScalarModularSubquery => s.children.nonEmpty
      case _ => false
    }.isDefined
  }
}

/**
 * A [[ListQuery]] expression defines the query which we want to search in an IN subquery
 * expression. It should and can only be used in conjunction with an IN expression.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   a.id IN (SELECT  id
 *                    FROM    b)
 * }}}
 */
case class ModularListQuery(
    plan: ModularPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends ModularSubquery(plan, children, exprId) with Unevaluable {
  override def dataType: DataType = plan.schema.fields.head.dataType
  override def nullable: Boolean = false
  override def withNewPlan(plan: ModularPlan): ModularListQuery = copy(plan = plan)
  override def toString: String = s"modular-list#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ModularListQuery(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}

/**
 * The [[Exists]] expression checks if a row exists in a subquery given some correlated condition.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   EXISTS (SELECT  *
 *                   FROM    b
 *                   WHERE   b.id = a.id)
 * }}}
 */
case class ModularExists(
    plan: ModularPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends ModularSubquery(plan, children, exprId) with Predicate with Unevaluable {
  override def nullable: Boolean = false
  override def withNewPlan(plan: ModularPlan): ModularExists = copy(plan = plan)
  override def toString: String = s"modular-exists#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ModularExists(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}
/**
 * A place holder for generated SQL for subquery expression.
 */
case class SubqueryHolder(override val sql: String) extends LeafExpression with Unevaluable {
  override def dataType: DataType = NullType
  override def nullable: Boolean = true
}

