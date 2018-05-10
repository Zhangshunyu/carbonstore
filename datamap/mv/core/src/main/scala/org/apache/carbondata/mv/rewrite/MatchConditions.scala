/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.rewrite

//import org.apache.carbondata.mv.plans.modular.ModularPlan

//TODO: implement this to modularize DefaultMatchingFunctions
object MatchConditions {
//  def setupMatchConditions(subsumer: SparkyPlan, subsumee: SparkyPlan, compensation: SparkyPlan): MatchConditions = {
//    val isUniqueRmE = subsumer.children.filter{x => subsumee.children.count(_ == x) > 1}
//    val isUniqueEmR = subsumee.children.filter{x => subsumer.children.count(_ == x) > 1}
//    
//        val extrajoin = subsumer.children.filterNot { child => subsumee.children.contains(child) }
//        val rejoin = subsumee.children.filterNot { child => subsumer.children.contains(child) }
//        val rejoinOutputList = rejoin.flatMap(_.output)
//        
//        val isPredicateRmE = subsumer.predicateList.forall(expr => subsumee.predicateList.exists(_.semanticEquals(expr)))
//        val isPredicateEmdR = subsumee.predicateList.forall(expr => subsumer.predicateList.exists(_.semanticEquals(expr)) || Utils.isDerivable(expr,subsumer.outputList ++ rejoinOutputList,subsumee,subsumer,None))
//        val isOutputEdR = subsumee.outputList.forall(expr => Utils.isDerivable(expr,subsumer.outputList ++ rejoinOutputList,subsumee,subsumer,None))
//
//        if (extrajoin.isEmpty && isPredicateRmE && isPredicateEmdR && isOutputEdR)
//    null
//  }
}

class MatchConditions(flags: Long) {
  def hasFlag(flag: Long): Boolean = ???
  
}