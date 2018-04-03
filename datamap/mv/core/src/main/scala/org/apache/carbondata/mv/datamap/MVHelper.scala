/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.mv.datamap

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{Field, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, DataMapSchemaStorageProvider, RelationIdentifier}
import org.apache.carbondata.mv.plans.modular.{GroupBy, Matchable, ModularPlan, Select}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility for MV datamap operations.
 */
object MVHelper {

  def createMVDataMap(sparkSession: SparkSession,
      dataMapSchema: DataMapSchema,
      queryString: String,
      storageProvider: DataMapSchemaStorageProvider,
      ifNotExistsSet: Boolean = false): Unit = {
    val dmProperties = dataMapSchema.getProperties.asScala
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val logicalPlan = sparkSession.sql(updatedQuery).drop("preAgg").queryExecution.analyzed
    val fields = logicalPlan.output.map { attr =>
      val name = updateColumnName(attr)
      val rawSchema = '`' + name + '`' + ' ' + attr.dataType.typeName
      if (attr.dataType.typeName.startsWith("decimal")) {
        val (precision, scale) = CommonUtil.getScaleAndPrecision(attr.dataType.catalogString)
        Field(column = name,
          dataType = Some(attr.dataType.typeName),
          name = Some(name),
          children = None,
          precision = precision,
          scale = scale,
          rawSchema = rawSchema)
      } else {
        Field(column = name,
          dataType = Some(attr.dataType.typeName),
          name = Some(name),
          children = None,
          rawSchema = rawSchema)
      }
    }
    val tableProperties = mutable.Map[String, String]()
    dmProperties.foreach(t => tableProperties.put(t._1, t._2))

    val selectTables = getTables(logicalPlan)

    // TODO inherit the table properties like sort order, sort scope and block size from parent
    // tables to mv datamap table
    // TODO Use a proper DB
    val tableIdentifier =
    TableIdentifier(dataMapSchema.getDataMapName + "_table",
      selectTables.head.identifier.database)
    // prepare table model of the collected tokens
    val tableModel: TableModel = new CarbonSpark2SqlParser().prepareTableModel(
      ifNotExistPresent = ifNotExistsSet,
      new CarbonSpark2SqlParser().convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      fields,
      Seq(),
      tableProperties,
      None,
      isAlterFlow = false,
      None)

    val tablePath = if (dmProperties.contains("path")) {
      dmProperties("path")
    } else {
      CarbonEnv.getTablePath(tableModel.databaseNameOp, tableModel.tableName)(sparkSession)
    }
    CarbonCreateTableCommand(TableNewProcessor(tableModel),
      tableModel.ifNotExistsSet, Some(tablePath), isVisible = false).run(sparkSession)

    dataMapSchema.setCtasQuery(queryString)
    dataMapSchema
      .setRelationIdentifier(new RelationIdentifier(tableIdentifier.database.get,
        tableIdentifier.table,
        ""))

    val parentIdents = selectTables.map { table =>
      new RelationIdentifier(table.database, table.identifier.table, "")
    }
    dataMapSchema.setParentTables(new util.ArrayList[RelationIdentifier](parentIdents.asJava))
    storageProvider.saveSchema(dataMapSchema)
  }

  def updateColumnName(attr: Attribute): String = {
    val name = attr.name.replace("(", "_").replace(")", "").replace(" ", "_").replace("=", "")
    attr.qualifier.map(qualifier => qualifier + "_" + name).getOrElse(name)
  }

  def getTables(logicalPlan: LogicalPlan): Seq[CatalogTable] = {
    logicalPlan.collect {
      case l: LogicalRelation => l.catalogTable.get
    }
  }

  def dropDummFuc(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case p@Project(exps, child) =>
        Project(dropDummyExp(exps), child)
      case Aggregate(grp, aggExp, child) =>
        Aggregate(
          grp,
          dropDummyExp(aggExp),
          child)
    }
  }

  private def dropDummyExp(exps: Seq[NamedExpression]) = {
    exps.map {
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase("preAgg") => None
      case other => Some(other)
    }.filter(_.isDefined).map(_.get)
  }

  def getAttributeMap(subsumer: Seq[NamedExpression],
      subsume: Seq[NamedExpression]): Map[AttributeKey, NamedExpression] = {
    if (subsumer.length == subsume.length) {
      subsume.zip(subsumer).flatMap { case (left, right) =>
        var tuples = left collect {
          case attr: AttributeReference =>
            (AttributeKey(attr), createAttrReference(right, attr.name))
        }
        left match {
          case a: Alias =>
            tuples = Seq((AttributeKey(a.child), createAttrReference(right, a.name))) ++ tuples
          case _ =>
        }
        Seq((AttributeKey(left), createAttrReference(right, left.name))) ++ tuples
      }.toMap
    } else {
      throw new UnsupportedOperationException("Cannot create mapping with unequal sizes")
    }
  }

  def createAttrReference(ref: NamedExpression, name: String): Alias = {
    Alias(ref, name)(exprId = ref.exprId, qualifier = None)
  }

  case class AttributeKey(exp: Expression) {
    override def hashCode(): Int = exp.hashCode()

    override def equals(obj: scala.Any): Boolean = {
      val otherAttr = obj.asInstanceOf[AttributeKey].exp
      exp.semanticEquals(otherAttr)
    }

  }

  /**
   * Updates the expressions as per the subsumer output expressions. It is needed to update the
   * expressions as per the datamap table relation
   *
   * @param expressions        expressions which are needed to update
   * @param aliasName          table alias name
   * @return Updated expressions
   */
  def updateSubsumeAttrs(
      expressions: Seq[Expression],
      attrMap: Map[AttributeKey, NamedExpression],
      aliasName: Option[String],
      keepAlias: Boolean = false): Seq[Expression] = {

    def updateAlias(alias: Alias,
        agg: AggregateExpression,
        name: String,
        aggFun: AggregateFunction) = {
      Alias(agg.copy(aggregateFunction = aggFun), name)(alias.exprId,
        alias.qualifier,
        alias.explicitMetadata,
        alias.isGenerated)
    }

    def getAttribute(exp: Expression) = {
      exp match {
        case Alias(agg: AggregateExpression, name) =>
          agg.aggregateFunction.collect {
            case attr: AttributeReference =>
              AttributeReference(attr.name, attr.dataType, attr.nullable, attr
                .metadata)(attr.exprId,
                aliasName,
                attr.isGenerated)
          }.head
        case Alias(child, name) =>
          child
        case other => other
      }
    }

    expressions.map {
        case alias@Alias(agg@AggregateExpression(sum: Sum, _, _, _), name) =>
          attrMap.get(AttributeKey(alias)).map{exp =>
            updateAlias(alias, agg, name, sum.copy(child = getAttribute(exp)))
          }.getOrElse(alias)
        case alias@Alias(agg@AggregateExpression(max: Max, _, _, _), name) =>
          attrMap.get(AttributeKey(alias)).map{exp =>
            updateAlias(alias, agg, name, max.copy(child = getAttribute(exp)))
          }.getOrElse(alias)

        case alias@Alias(agg@AggregateExpression(min: Min, _, _, _), name) =>
          attrMap.get(AttributeKey(alias)).map{exp =>
            updateAlias(alias, agg, name, min.copy(child = getAttribute(exp)))
          }.getOrElse(alias)

        case alias@Alias(agg@AggregateExpression(count: Count, _, _, _), name) =>
          attrMap.get(AttributeKey(alias)).map{exp =>
            updateAlias(alias, agg, name, Sum(getAttribute(exp)))
          }.getOrElse(alias)

        case alias@Alias(agg@AggregateExpression(avg: Average, _, _, _), name) =>
          // TODO need to support average in while creating table and quaerying as well
          attrMap.get(AttributeKey(alias)).map{exp =>
            updateAlias(alias, agg, name, avg.copy(child = getAttribute(exp)))
          }.getOrElse(alias)

        case attr: AttributeReference =>
          val uattr = attrMap.get(AttributeKey(attr)).map{a =>
            if (keepAlias) {
              AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId,
                attr.qualifier,
                a.isGenerated)
            } else {
              a
            }
          }.getOrElse(attr)
          uattr
        case expression: Expression =>
          val uattr = attrMap.get(AttributeKey(expression)).getOrElse(expression)
          uattr
    }
  }

  def updateOutPutList(
      subsumerOutputList: Seq[NamedExpression],
      dataMapRltn: Select,
      aliasMap: Map[AttributeKey, NamedExpression],
      keepAlias: Boolean): Seq[NamedExpression] = {
    var outputSel =
      updateSubsumeAttrs(
        subsumerOutputList,
        aliasMap,
        Some(dataMapRltn.aliasMap.values.head),
        keepAlias).asInstanceOf[Seq[NamedExpression]]
    outputSel.zip(subsumerOutputList).map{ case (l, r) =>
      l match {
        case attr: AttributeReference =>
          Alias(attr, r.name)(r.exprId, None)
        case a@Alias(attr: AttributeReference, name) =>
          Alias(attr, r.name)(r.exprId, None)
        case other => other
      }
    }

  }

  def updateSelectPredicates(
      predicates: Seq[Expression],
      attrMap: Map[AttributeKey, NamedExpression],
      keepAlias: Boolean): Seq[Expression] = {
    predicates.map { exp =>
      exp transform {
        case attr: AttributeReference =>
          val uattr = attrMap.get(AttributeKey(attr)).map{a =>
            if (keepAlias) {
              AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId,
                attr.qualifier,
                a.isGenerated)
            } else {
              a
            }
          }.getOrElse(attr)
          uattr
      }
    }
  }

  /**
   * Update the modular plan as per the datamap table relation inside it.
   *
   * @param subsumer plan to be updated
   * @return Updated modular plan.
   */
  def updateDataMap(subsumer: ModularPlan): ModularPlan = {
    subsumer match {
      case select: Select if select.children.nonEmpty && select.dataMapTableRelation.isEmpty =>
        select.children match {
          case Seq(s: Select) if s.dataMapTableRelation.isDefined =>
            val relation = s.dataMapTableRelation.get.asInstanceOf[Select]
            val child = updateDataMap(s).asInstanceOf[Select]
            val aliasMap = getAttributeMap(relation.outputList, s.outputList)
            var outputSel =
              updateOutPutList(select.outputList, relation, aliasMap, keepAlias = true)

            val pred = updateSelectPredicates(select.predicateList, aliasMap, true)
            select.copy(outputList = outputSel,
              inputList = child.outputList,
              predicateList = pred,
              children = Seq(child))
          case Seq(g: GroupBy) if g.dataMapTableRelation.isDefined =>
            val relation = g.dataMapTableRelation.get.asInstanceOf[Select]
            val aliasMap = getAttributeMap(relation.outputList, g.outputList)

            val outputSel =
              updateOutPutList(select.outputList, relation, aliasMap, keepAlias = false)
            val child = updateDataMap(g).asInstanceOf[GroupBy]
            // TODO Remove the unnecessary columns from selection.
            // Only keep columns which are required by parent.
            val inputSel = child.outputList
            select
              .copy(outputList = outputSel,
                inputList = inputSel,
                children = Seq(child))

          case _ => select
        }
      case group: GroupBy if group.children.nonEmpty && group.dataMapTableRelation.isEmpty =>
        group.children match {
          case Seq(s: Select) if s.dataMapTableRelation.isDefined =>
            val relation = s.dataMapTableRelation.get.asInstanceOf[Select]
            val child = updateDataMap(s).asInstanceOf[Select]
            val aliasMap = getAttributeMap(relation.outputList, s.outputList)
            val outputSel = updateSubsumeAttrs(group.outputList,
              aliasMap,
              Some(relation.aliasMap.values.head), true).asInstanceOf[Seq[NamedExpression]]
            val pred = updateSubsumeAttrs(group.predicateList, aliasMap,
              Some(relation.aliasMap.values.head), true)
            group.copy(outputList = outputSel,
              inputList = child.outputList,
              predicateList = pred,
              child = child)
          case Seq(g: GroupBy) if g.dataMapTableRelation.isDefined =>
            val relation = g.dataMapTableRelation.get.asInstanceOf[Select]
            val attrMap = getAttributeMap(relation.outputList, group.outputList)
            val outputSel =
              updateSubsumeAttrs(
                group.outputList,
                attrMap,
                Some(relation.aliasMap.values.head)).asInstanceOf[Seq[NamedExpression]]

            val childL = updateDataMap(g).asInstanceOf[GroupBy]
            // TODO Remove the unnecessary columns from selection.
            // Only keep columns which are required by parent.
            val inputSel = childL.outputList
            group
              .copy(outputList = outputSel,
                inputList = inputSel,
                child = childL)

          case _ => group
        }
      case s: Select if s.dataMapTableRelation.isDefined =>
        val relation = s.dataMapTableRelation.get.asInstanceOf[Select]
        val attrMap = getAttributeMap(relation.outputList, s.outputList)
        val out =
          updateSubsumeAttrs(
            s.outputList,
            attrMap,
            Some(relation.aliasMap.values.head)).asInstanceOf[Seq[NamedExpression]]
        val in = relation.asInstanceOf[Select].outputList
        s.copy(outputList = out,
            inputList = in,
            predicateList = Seq.empty,
            aliasMap = relation.aliasMap,
            joinEdges = Seq.empty,
            children = Seq(relation),
            dataMapTableRelation = None)
      case g: GroupBy if g.dataMapTableRelation.isDefined =>
        val relation = g.dataMapTableRelation.get.asInstanceOf[Select]
        val attrMap = getAttributeMap(relation.outputList, g.outputList)
        val out =
          updateSubsumeAttrs(
            g.outputList,
            attrMap,
            Some(relation.aliasMap.values.head)).asInstanceOf[Seq[NamedExpression]]
        val in = relation.asInstanceOf[Select].outputList
        val pred =
          updateSubsumeAttrs(g.predicateList,
            attrMap,
            Some(relation.aliasMap.values.head)).map {
          case alias: Alias =>
            alias.child
          case other => other
        }
        g.copy(outputList = out,
          inputList = in,
          predicateList = pred,
          child = relation,
          dataMapTableRelation = None)
      case other => other
    }
  }
}

