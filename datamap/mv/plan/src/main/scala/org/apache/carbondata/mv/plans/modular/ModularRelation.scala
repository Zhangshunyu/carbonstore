/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans.modular

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.spark.sql.catalyst.expressions.aggregate._

object ModularRelation {
  def apply(outputList: NamedExpression*): ModularRelation = new ModularRelation("test", "test", outputList, NoFlags, Seq.empty)
}

case class ModularRelation(databaseName: String, tableName: String, outputList: Seq[NamedExpression], flags: FlagSet, rest: Seq[Seq[Any]]) extends LeafNode {
  override def computeStats(spark: SparkSession, conf: SQLConf): Statistics = {
    val plan = spark.table(s"${databaseName}.${tableName}").queryExecution.optimizedPlan
    val stats = plan.stats(conf)
    val output = outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq(0)
    val attributeStats = AttributeMap(stats.attributeStats.iterator.map{pair => (rewrites(pair._1), pair._2)}.toSeq)
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats, stats.hints) 
  }
  
  override def output: Seq[Attribute] = outputList.map(_.toAttribute)
  override def adjacencyList: Map[Int, Seq[(Int, JoinType)]] = Map.empty
  override def equals(that: Any): Boolean = that match {
    case that: ModularRelation => {
      if ((databaseName != null && tableName != null && databaseName == that.databaseName && tableName == that.tableName) ||
        (databaseName == null && tableName == null && that.databaseName == null && that.tableName == null && output.toString == (that.output).toString))
        true
      else false
    }
    case _ => false
  }
}

//trait HarmonizedModularRelation {
//  type HarmonizedModularPlan = {
//    GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, dim::Nil, NoFlags, Nil, Nil), NoFlags, Nil)
//  }
//}
object HarmonizedRelation {
  def canHarmonize(source: ModularPlan): Boolean = source match {
    case g @ GroupBy(_, _, _, _, Select(_, _, _, _, _, dim::Nil, NoFlags, Nil, Nil), NoFlags, Nil) if (dim.isInstanceOf[ModularRelation]) => {
      if (g.outputList.forall(col => {
        if (col.isInstanceOf[Alias]) {
          val check = (col.asInstanceOf[Alias].child.isInstanceOf[AttributeReference] || 
                       col.asInstanceOf[Alias].child.isInstanceOf[Literal] || 
                       (col.asInstanceOf[Alias].child match {
                          case AggregateExpression(First(_,_),_,_,_) => true
                          case AggregateExpression(Last(_,_),_,_,_) => true
                          case _ => false
                       })) 
          check
        }
        else {
          val check = (col.isInstanceOf[AttributeReference] ||
                       col.isInstanceOf[Literal] || 
                       (col.asInstanceOf[Expression] match {
                          case AggregateExpression(First(_,_),_,_,_) => true
                          case AggregateExpression(Last(_,_),_,_,_) => true
                          case _ => false                
                       })) 
          check
        }
      })) true
      else false
    }
    case _ => false
  }
}

// support harmonization for dimension table
case class HarmonizedRelation(source: ModularPlan) extends LeafNode {
  require(HarmonizedRelation.canHarmonize(source), "invalid plan for harmonized relation")
  lazy val tableName = source.asInstanceOf[GroupBy].child.children(0).asInstanceOf[ModularRelation].tableName
  lazy val databaseName = source.asInstanceOf[GroupBy].child.children(0).asInstanceOf[ModularRelation].databaseName
  lazy val tag: Option[Attribute] = {
    source match {
      case GroupBy(head::tail,_,_,_,_,_,_) if (head.isInstanceOf[Alias] && head.asInstanceOf[Alias].child == Literal(1)) => Some(head.toAttribute)
      case _ => None
    }
  }
//  override def computeStats(spark: SparkSession, conf: SQLConf): Statistics = source.stats(spark, conf)
  override def computeStats(spark: SparkSession, conf: SQLConf): Statistics = {
    val plan = spark.table(s"${databaseName}.${tableName}").queryExecution.optimizedPlan
    val stats = plan.stats(conf)
    val output = source.asInstanceOf[GroupBy].child.children(0).asInstanceOf[ModularRelation].outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq(0)
    val aliasMap = AttributeMap(
        source.asInstanceOf[GroupBy].outputList.collect {
          case a @ Alias(ar:Attribute,_) => (ar, a.toAttribute)
          case a @ Alias(AggregateExpression(First(ar:Attribute,_),_,_,_),_) => (ar, a.toAttribute)
          case a @ Alias(AggregateExpression(Last(ar:Attribute,_),_,_,_),_) => (ar, a.toAttribute)
        })
    val aStatsIterator = stats.attributeStats.iterator.map{pair => (rewrites(pair._1), pair._2)}      
    val attributeStats = AttributeMap(aStatsIterator.map(pair => ((aliasMap.get(pair._1)).getOrElse(pair._1), pair._2)).toSeq)
    
    Statistics(stats.sizeInBytes, None, attributeStats, stats.hints)
  }
  override def output: Seq[Attribute] = source.output
  // two harmonized modular relations are equal only if orders of output columns of 
  // their source plans are exactly the same
  override def equals(that: Any): Boolean = {
    def canonicalize(source: ModularPlan): ModularPlan = {
      source.transform { case gb @ GroupBy(head::tail,_,_,_,_,_,_) if (head.isInstanceOf[Alias] && head.asInstanceOf[Alias].child == Literal(1)) => gb.copy(outputList = tail) }
    }

    that match {
      case that: HarmonizedRelation => { 
        val s1 = canonicalize(source)
        val s2 = canonicalize(that.source)
        val r = s1.sameResult(s2)
        r }
      case _ => false
    }
  }
  
  def addTag: HarmonizedRelation = {
    val source1 = source.transform { case gb @ GroupBy(head::tail,_,_,_,_,_,_) if (!head.isInstanceOf[Alias] || !(head.asInstanceOf[Alias].child == Literal(1))) => {
      val exprId = NamedExpression.newExprId
      gb.copy(outputList = Alias(Literal(1), s"gen_tag_${exprId.id}")(exprId) +: gb.outputList) }
    }
    HarmonizedRelation(source1)
  }
  
  def hasTag: Boolean = tag match {
    case Some(_) => true
    case None => false
  }
    
}