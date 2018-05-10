/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.rewrite

import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.plans.modular
//import org.apache.carbondata.mv.plans.modular.LeafNode
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.Attribute
//import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.carbondata.mv.session.MVSession
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.carbondata.mv.expressions.modular._

private[mv] class Navigator(catalog: SummaryDatasetCatalog, session: MVSession) {
  
  def rewriteWithSummaryDatasets(plan: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    val replaced = plan.transformAllExpressions {
      case s: ModularSubquery =>
        if (s.children.isEmpty) {
          rewriteWithSummaryDatasetsCore(s.plan, rewrite) match {
            case Some(rewrittenPlan) => ScalarModularSubquery(rewrittenPlan, s.children, s.exprId)
            case None => s
          }
        }
        else throw new UnsupportedOperationException(s"Rewrite expression $s isn't supported")        
      case o => o
    }
    rewriteWithSummaryDatasetsCore(replaced, rewrite).getOrElse(replaced)
  }

  def rewriteWithSummaryDatasetsCore(plan: ModularPlan, rewrite: QueryRewrite): Option[ModularPlan] = {
    val rewrittenPlan = plan transformDown {
      case currentFragment =>
        if (currentFragment.rewritten || !currentFragment.isSPJGH) currentFragment
        else {
          val compensation = (for { dataset <- catalog.lookupFeasibleSummaryDatasets(currentFragment).toStream
                                    subsumer <- session.sessionState.modularizer.modularize(session.sessionState.optimizer.execute(dataset.plan)) //.map(_.harmonized)
                                    subsumee <- unifySubsumee(currentFragment)
                                    comp <- subsume(unifySubsumer2(unifySubsumer1(subsumer,subsumee), subsumee).setSkip, subsumee, rewrite) 
                                  } yield comp).headOption
          compensation.map(_.setRewritten).getOrElse(currentFragment)
        }
    }
    if (rewrittenPlan.fastEquals(plan)) None
    else Some(rewrittenPlan)
  }

  def subsume(subsumer: ModularPlan, subsumee: ModularPlan, rewrite: QueryRewrite): Option[ModularPlan] = {
    if (subsumer.getClass == subsumee.getClass) {
      (subsumer.children, subsumee.children) match {
        case (Nil, Nil) => None
        case (r, e) if (r.forall(_.isInstanceOf[modular.LeafNode]) && e.forall(_.isInstanceOf[modular.LeafNode])) => {
          val iter = session.sessionState.matcher.execute(subsumer, subsumee, None, rewrite)
          if (iter.hasNext) Some(iter.next)
          else None
        }
        case (rchild :: Nil, echild :: Nil) => {
          val compensation = subsume(rchild, echild, rewrite)
          val oiter = compensation.map {
            case comp if (comp.eq(rchild)) => session.sessionState.matcher.execute(subsumer, subsumee, None, rewrite)
            case _ => session.sessionState.matcher.execute(subsumer, subsumee, compensation, rewrite)
          }
          oiter.flatMap { case iter if iter.hasNext => Some(iter.next)
                          case _ => None }
        }
        case _ => None
      }
    } else None
  }

  // add Select operator as placeholder on top of subsumee to facilitate matching
  def unifySubsumee(subsumee: ModularPlan): Option[ModularPlan] = {
    subsumee match {
      case gb @ modular.GroupBy(_,_,_,_,modular.Select(_,_,_,_,_,_,_,_,_),_,_) =>
        Some(modular.Select(gb.outputList,gb.outputList,Nil,Map.empty,Nil,gb :: Nil,gb.flags,gb.flagSpec,Seq.empty))
      case other => Some(other)
    }
  }
  
  // add Select operator as placeholder on top of subsumer to facilitate matching
  def unifySubsumer1(subsumer: ModularPlan, subsumee: ModularPlan): ModularPlan = {
    (subsumer,subsumee) match {       
      case (r @ modular.GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, _,_,_,_),_,_),modular.Select(_,_,_,_,_,Seq(modular.GroupBy(_,_,_,_,modular.Select(_,_,_,_,_,_,_,_,_),_,_)),_,_,_)) =>
        modular.Select(r.outputList,r.outputList,Nil,Map.empty,Nil,r :: Nil,r.flags,r.flagSpec,Seq.empty)
      case _ => subsumer
    }
  }
  
  def unifySubsumer2(subsumer: ModularPlan, subsumee: ModularPlan): ModularPlan = {
    val rtables = subsumer.collect { case n: modular.LeafNode => n }
    val etables = subsumee.collect { case n: modular.LeafNode => n }
    val pairs = for {
      rtable <- rtables
      etable <- etables
      if (rtable == etable)
    } yield (rtable, etable)

    pairs.foldLeft(subsumer) {
      case (curSubsumer, pair) => {
        val nxtSubsumer = curSubsumer.transform { case pair._1 => pair._2 }
        val attributeSet = AttributeSet(pair._1.output)
        val rewrites = AttributeMap(pair._1.output.zip(pair._2.output))
        nxtSubsumer.transformUp({
          case p => p.transformExpressions({
            case a: Attribute if attributeSet contains a => rewrites(a).withQualifier(a.qualifier)
          })
        })
      }
    }
  }
}
