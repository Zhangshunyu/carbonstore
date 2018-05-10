/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans.modular

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.util.SignatureGenerator
import org.apache.carbondata.mv.plans.util.SignatureRule
import org.apache.carbondata.mv.plans.util.Signature

object ModularPlanSignatureGenerator extends SignatureGenerator[ModularPlan] {
  lazy val rule: SignatureRule[ModularPlan] = ModularPlanRule

  override def generate(plan: ModularPlan): Option[Signature] = {
    if (plan.isSPJGH) super.generate(plan)
    else None
  }
}

object ModularPlanRule extends SignatureRule[ModularPlan] {

  def apply(plan: ModularPlan, childSignatures: Seq[Option[Signature]]): Option[Signature] = {

    plan match {
      case modular.Select(_, _, _, _, _, _, _, _, _) =>
        if (childSignatures.map { _.getOrElse(Signature()).groupby }.forall(x => !x)) {
          Some(Signature(false, childSignatures.flatMap { _.getOrElse(Signature()).datasets.toSeq }.toSet))
        } else if (childSignatures.length == 1 && childSignatures(0).getOrElse(Signature()).groupby) {
          childSignatures(0)
        } else {
          None
        }
      case modular.GroupBy(_, _, _, _, _, _, _) =>
        if (childSignatures.length == 1 && !childSignatures(0).getOrElse(Signature()).groupby) {
          Some(Signature(true, childSignatures(0).getOrElse(Signature()).datasets))
        } else {
          None
        }
      case modular.HarmonizedRelation(source) =>
        source.signature match {
          case Some(s) =>
            Some(Signature(false, s.datasets))
          case _ =>
            None
        }
      case modular.ModularRelation(dbname, tblname, _, _, _) =>
        if (dbname != null && tblname != null)
          Some(Signature(false, Set(Seq(dbname, tblname).mkString("."))))
        else Some(Signature(false, Set(plan.toString())))
      case _ => None
    }
  }
}