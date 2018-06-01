package org.apache.carbondata.mv.plans.modular

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.plans._
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans._

abstract class Harmonizer(conf: SQLConf) 
  extends RuleExecutor[ModularPlan] {
  
//  protected val fixedPoint = FixedPoint(conf.getConfString("spark.mv.harmonizer.maxIterations").toInt)
  protected val fixedPoint = FixedPoint(conf.optimizerMaxIterations)
      
  def batches: Seq[Batch] = {
    Batch("Data Harmonizations", fixedPoint, Seq(
        HarmonizeDimensionTable) ++
        extendedOperatorHarmonizationRules: _*) :: Nil
//        HarmonizeFactTable) :: Nil
  }
  
  /**
   * Override to provide additional rules for the modular operator harmonization batch.
   */
  def extendedOperatorHarmonizationRules: Seq[Rule[ModularPlan]] = Nil
}
  
/**
 * A full Harmonizer - harmonize both fact and dimension tables
 */
object FullHarmonizer extends FullHarmonizer
    
class FullHarmonizer extends Harmonizer(new SQLConf()) {
  override def extendedOperatorHarmonizationRules: Seq[Rule[ModularPlan]] =
    super.extendedOperatorHarmonizationRules ++ (HarmonizeFactTable :: Nil)
}

/**
 * A semi Harmonizer - harmonize dimension tables only
 */
object SemiHarmonizer extends SemiHarmonizer
    
class SemiHarmonizer extends Harmonizer(new SQLConf()) 


object HarmonizeDimensionTable extends Rule[ModularPlan] with PredicateHelper {

  def apply(plan: ModularPlan): ModularPlan = plan transform {
    case s @ Select(_, _, _, _, jedges, fact :: dims, _, _, _) if jedges.forall(e => e.joinType == LeftOuter || e.joinType == Inner) &&
      fact.isInstanceOf[modular.ModularRelation] && dims.filterNot(_.isInstanceOf[modular.LeafNode]).nonEmpty &&
      dims.forall(d => (d.isInstanceOf[modular.ModularRelation] || HarmonizedRelation.canHarmonize(d))) => {
        var tPullUpPredicates = Seq.empty[Expression]
        val tChildren = fact :: dims.map { 
          case m: modular.ModularRelation => m 
          case h @ GroupBy(_, _, _, _, s1 @ Select(_, _, _, _, _, dim::Nil, NoFlags, Nil, Nil), NoFlags, Nil) if (dim.isInstanceOf[ModularRelation]) => {
            val rAliasMap = AttributeMap(h.outputList.collect { case a: Alias if a.child.isInstanceOf[Attribute] => (a.child.asInstanceOf[Attribute], a.toAttribute) })
            val pullUpPredicates = s1.predicateList.map(replaceAlias(_, rAliasMap.asInstanceOf[AttributeMap[Expression]]))
            if (pullUpPredicates.forall(cond => canEvaluate(cond, h))) {
              tPullUpPredicates = tPullUpPredicates ++ pullUpPredicates
              HarmonizedRelation(h.copy(child=s1.copy(predicateList=Nil)))
            }
            else h
            }
          //case _ => 
          }
        if (tChildren.forall(_.isInstanceOf[modular.LeafNode])) {
          s.copy(predicateList = s.predicateList ++ tPullUpPredicates, children = tChildren)
        }
        else s
        }
//        s.withNewChildren(fact :: dims.map { case m: modular.ModularRelation => m; case h => HarmonizedRelation(h) })}
//        s.copy(predicateList = predicateList ++ moveUpPredicates, children = tChildren)} //fact :: dims.map { case m: modular.ModularRelation => m; case h => HarmonizedRelation(h) })}
  }

}

object HarmonizeFactTable extends Rule[ModularPlan] with PredicateHelper with AggregatePushDown {

  def apply(plan: ModularPlan): ModularPlan = plan transform {
    case g @ GroupBy(_, _, _, _, s @ Select(_, _, _, aliasm, jedges, fact :: dims, _, _, _), _, _) if s.adjacencyList.keySet.size <= 1 &&
      jedges.forall(e => e.joinType == Inner) && // || e.joinType == LeftOuter) && // !s.flags.hasFlag(DISTINCT) &&
      fact.isInstanceOf[modular.ModularRelation] && (fact :: dims).forall(_.isInstanceOf[modular.LeafNode]) && dims.nonEmpty => {
      val selAliasMap = AttributeMap(s.outputList.collect {
        case a: Alias if (a.child.isInstanceOf[Attribute]) => (a.toAttribute, a.child.asInstanceOf[Attribute])
      })
      val aggTransMap = findPushThroughAggregates(g.outputList,
        selAliasMap,
        fact.asInstanceOf[modular.ModularRelation])

      val constraintsAttributeSet = dims.flatMap(s.extractEvaluableConditions(_)).map(_.references).foldLeft(AttributeSet.empty)(_ ++ _)
      val groupingAttributeSet = g.predicateList.map(_.references).foldLeft(AttributeSet.empty)(_ ++ _)
      if (aggTransMap.isEmpty ||
        //TODO: the following condition is too pessimistic, more work needed using methods similar to those in trait 
        //      QueryPlanConstraints 
        !constraintsAttributeSet.subsetOf(groupingAttributeSet))
        g
      else {
        val starJExprs = dims.flatMap(dim => s.extractJoinConditions(fact, dim)).toSeq
        val gJAttributes = starJExprs.map(expr => expr.references).foldLeft(AttributeSet.empty)(_ ++ _).filter(fact.outputSet.contains(_))
        val fExprs = s.extractEvaluableConditions(fact)
        val gFAttributes = fExprs.map(expr => expr.references).foldLeft(AttributeSet.empty)(_ ++ _).filter(fact.outputSet.contains(_))
        val gGAttributes = g.predicateList.map(expr => expr.references).foldLeft(AttributeSet.empty)(_ ++ _).filter(fact.outputSet.contains(_))
        val gAttributes = (gJAttributes ++ gFAttributes ++ gGAttributes).toSeq.sortBy(_.name)  //sortBy is to facilitate running regression

        val oAggregates = aggTransMap.map(_._2).flatMap(_._2).toSeq

        val tAliasMap = (aliasm.get(0) match { case Some(name) => Seq((0, name)); case _ => Seq.empty }).toMap
        val sOutput = (oAggregates.map(_.references).foldLeft(AttributeSet.empty)(_ ++ _) ++ AttributeSet(gAttributes)).toSeq
        val hFactSel = modular.Select(sOutput, fact.output, Seq.empty, tAliasMap, Seq.empty, fact :: Nil, NoFlags, Seq.empty, Seq.empty)
        val hFact = modular.GroupBy(gAttributes ++ oAggregates, sOutput, gAttributes, None, hFactSel, NoFlags, Seq.empty)
        val hFactName = s"gen_harmonized_${fact.asInstanceOf[modular.ModularRelation].databaseName}_${fact.asInstanceOf[modular.ModularRelation].tableName}"
        val hAliasMap = (aliasm - 0) + (0 -> hFactName)
        val hInputList = gAttributes ++ oAggregates.map(_.toAttribute) ++ dims.flatMap(_.asInstanceOf[modular.LeafNode].output).toSeq
        // val hPredicateList = s.predicateList
        val attrOutputList = s.outputList.filter(expr => (expr.isInstanceOf[Attribute]) ||
          (expr.isInstanceOf[Alias] && expr.asInstanceOf[Alias].child.isInstanceOf[Attribute]))
        val aggOutputList = aggTransMap.values.flatMap(t => t._2).map { ref => AttributeReference(ref.name, ref.dataType)(exprId = ref.exprId, qualifier = Some(hFactName)) }
        val hOutputList = attrOutputList ++ aggOutputList
        val hSel = s.copy(outputList = hOutputList, inputList = hInputList, aliasMap = hAliasMap, children = hFact :: dims)
        val gOutputList = g.outputList.zipWithIndex.map { case (expr, index) => if (aggTransMap.keySet.contains(index)) aggTransMap(index)._1 else expr }

        val wip = g.copy(outputList = gOutputList, inputList = hInputList, child = hSel)
        val hFactOutputSet = hFact.outputSet
        wip.transformExpressions {
          case ref: Attribute if hFactOutputSet.contains(ref) =>
            AttributeReference(ref.name, ref.dataType)(exprId = ref.exprId, qualifier = Some(hFactName))
        }
      }
    }
  }
}
 
