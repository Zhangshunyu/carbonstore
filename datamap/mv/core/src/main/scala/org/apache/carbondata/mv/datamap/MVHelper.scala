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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{Field, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, DataMapSchemaStorageProvider, RelationIdentifier}
import org.apache.carbondata.mv.plans.modular.{GroupBy, ModularPlan, Select}
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
      subsume: Seq[NamedExpression]): Map[String, NamedExpression] = {
    if (subsumer.length == subsume.length) {
      subsume.zip(subsumer).flatMap { case (left, right) =>
        val tuples = left collect {
          case attr: AttributeReference =>
            (attr.name, right)
        }
        tuples
        Seq((left.name, right)) ++ tuples
      }.toMap
    } else {
      throw new UnsupportedOperationException("Cannot create mapping with unequal sizes")
    }
  }

  /**
   * Updates the expressions as per the subsumer output expressions. It is needed to update the
   * expressions as per the datamap table relation
   *
   * @param expressions        expressions which are needed to update
   * @param subSumeoutPutList  Current subsumee output for mapping
   * @param subSumerOutPutList Current subsumer output for mapping
   * @param aliasName          table alias name
   * @return Updated expressions
   */
  def updateSubsumeAttrs(
      expressions: Seq[Expression],
      subSumeoutPutList: Seq[NamedExpression],
      subSumerOutPutList: Seq[NamedExpression],
      aliasName: Option[String]): Seq[Expression] = {
    val attrMap = getAttributeMap(subSumerOutPutList, subSumeoutPutList)

    def updateAlias(alias: Alias,
        agg: AggregateExpression,
        name: String,
        aggFun: AggregateFunction) = {
      Alias(agg.copy(aggregateFunction = aggFun), name)(alias.exprId,
        alias.qualifier,
        alias.explicitMetadata,
        alias.isGenerated)
    }

    def getAttribute(exp: NamedExpression) = {
      exp match {
        case Alias(agg: AggregateExpression, name) =>
          agg.aggregateFunction.collect {
            case attr: AttributeReference =>
              AttributeReference(attr.name, attr.dataType, attr.nullable, attr
                .metadata)(attr.exprId,
                aliasName,
                attr.isGenerated)
          }.head
        case other => other
      }
    }

    expressions.map { exp =>
      var needUpdate = true
      exp transform {
        case alias@Alias(agg@AggregateExpression(sum: Sum, _, _, _), name) if needUpdate =>
          needUpdate = false
          attrMap.get(name).map{exp =>
            updateAlias(alias, agg, name, sum.copy(child = getAttribute(exp)))
          }.getOrElse(alias)
        case alias@Alias(agg@AggregateExpression(max: Max, _, _, _), name) if needUpdate =>
          needUpdate = false
          attrMap.get(name).map{exp =>
            updateAlias(alias, agg, name, max.copy(child = getAttribute(exp)))
          }.getOrElse(alias)

        case alias@Alias(agg@AggregateExpression(min: Min, _, _, _), name) if needUpdate =>
          needUpdate = false
          attrMap.get(name).map{exp =>
            updateAlias(alias, agg, name, min.copy(child = getAttribute(exp)))
          }.getOrElse(alias)

        case alias@Alias(agg@AggregateExpression(count: Count, _, _, _), name) if needUpdate =>
          needUpdate = false
          attrMap.get(name).map{exp =>
            updateAlias(alias, agg, name, Sum(exp))
          }.getOrElse(alias)

        case alias@Alias(agg@AggregateExpression(avg: Average, _, _, _), name) if needUpdate =>
          // TODO need to support average in while creating table and quaerying as well
          needUpdate = false
          attrMap.get(name).map{exp =>
            updateAlias(alias, agg, name, avg.copy(child = getAttribute(exp)))
          }.getOrElse(alias)

        case attr: AttributeReference if needUpdate =>
          needUpdate = false
          val uattr = attrMap.getOrElse(attr.name, attr)
          AttributeReference(uattr.name, attr.dataType, attr.nullable, attr
            .metadata)(attr.exprId,
            aliasName,
            attr.isGenerated)
      }
    }
  }

  /**
   * Update the modular plan as per the datamap table relation inside it.
   *
   * @param subsumer
   * @param updateDirect it directly keeps the relation under modular plan as child in case of true.
   * @return Updated modular plan.
   */
  def updateDataMap(subsumer: ModularPlan, updateDirect: Boolean = false): ModularPlan = {
    subsumer match {
      case select: Select if select.children.nonEmpty && select.dataMapTableRelation.isEmpty =>
        select.children.head match {
          case s: Select if s.dataMapTableRelation.isDefined =>
            val relation = s.dataMapTableRelation.get.asInstanceOf[Select]
            val outputSel = updateSubsumeAttrs(select.outputList,
              select.outputList,
              relation.outputList,
              Some(relation.aliasMap.values.head))
              .asInstanceOf[Seq[NamedExpression]]
            val inputSel = relation.outputList
            select.copy(outputList = outputSel,
              inputList = inputSel,
              children = relation.children)
          case g: GroupBy if g.dataMapTableRelation.isDefined =>
            val relation = g.dataMapTableRelation.get.asInstanceOf[Select]
            val out = updateSubsumeAttrs(g.outputList,
              g.outputList,
              relation.outputList,
              Some(relation.aliasMap.values.head))
              .asInstanceOf[Seq[NamedExpression]]
            val in = relation.outputList
            val pred = updateSubsumeAttrs(g.predicateList,
              g.outputList,
              relation.outputList,
              Some(relation.aliasMap.values.head))
            val outputSel = updateSubsumeAttrs(select.outputList,
              select.outputList,
              relation.outputList,
              Some(relation.aliasMap.values.head))
              .asInstanceOf[Seq[NamedExpression]]
            if (updateDirect) {
              val child = g.copy(outputList = out,
                inputList = in,
                predicateList = pred,
                child = relation,
                dataMapTableRelation = None)
              val inputSel = child.outputList
              select
                .copy(outputList = outputSel,
                  inputList = inputSel,
                  children = Seq(child))
            } else {
              select
                .copy(outputList = outputSel,
                  inputList = in,
                  children = Seq(relation))
            }

          case _ => select
        }
      case s: Select if s.dataMapTableRelation.isDefined =>
        val relation = s.dataMapTableRelation.get.asInstanceOf[Select]
        val out = updateSubsumeAttrs(s.outputList,
          s.outputList,
          relation.outputList,
          Some(relation.aliasMap.values.head))
          .asInstanceOf[Seq[NamedExpression]]
        val in = relation.asInstanceOf[Select].outputList
        s
          .copy(outputList = out,
            inputList = in,
            predicateList = Seq.empty,
            aliasMap = relation.aliasMap,
            joinEdges = Seq.empty,
            children = Seq(relation),
            dataMapTableRelation = None)
      case g: GroupBy if g.dataMapTableRelation.isDefined =>
        val relation = g.dataMapTableRelation.get.asInstanceOf[Select]
        val out = updateSubsumeAttrs(g.outputList,
          g.outputList,
          relation.outputList,
          Some(relation.aliasMap.values.head)).asInstanceOf[Seq[NamedExpression]]
        val in = relation.asInstanceOf[Select].outputList
        val pred = updateSubsumeAttrs(g.predicateList,
          g.outputList,
          relation.outputList,
          Some(relation.aliasMap.values.head))
        if (updateDirect) {
          g.copy(outputList = out,
            inputList = in,
            predicateList = pred,
            child = relation,
            dataMapTableRelation = None)
        } else {
          relation
        }
      case other => other
    }
  }
}

