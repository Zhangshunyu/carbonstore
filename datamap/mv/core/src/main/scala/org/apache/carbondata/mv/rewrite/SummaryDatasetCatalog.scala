/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.rewrite

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.FindDataSourceTable
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.carbondata.mv.plans.util.Signature
import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.plans.util.BirdcageOptimizer
import org.apache.carbondata.mv.plans.modular.SimpleModularizer

import org.apache.carbondata.core.datamap.DataMapCatalog
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.mv.plans.modular.{Flags, ModularPlan, ModularRelation, Select}
import org.apache.carbondata.mv.plans.util.Signature
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.{Alias, ScalaUDF}
import org.apache.spark.sql.catalyst.plans._



/** Holds a summary logical plan */
private[mv] case class SummaryDataset(signature: Option[Signature], 
    plan: LogicalPlan, 
    dataMapSchema: DataMapSchema, 
    mvRepresentation: LogicalPlan)

private[mv] class SummaryDatasetCatalog(sparkSession: SparkSession)
  extends DataMapCatalog[SummaryDataset] {
  
  @transient
  private val summaryDatasets = new scala.collection.mutable.ArrayBuffer[SummaryDataset]

  @transient
  private val registerLock = new ReentrantReadWriteLock
  
  /**
   * parser
   */
  lazy val parser = new CarbonSpark2SqlParser  
  
  private val optimizer = BirdcageOptimizer  
  
  private val modularizer = SimpleModularizer

//  /** Returns true if the table is currently registered in-catalog. */
//  def isRegistered(tableName: String): Boolean = lookupSummaryDataset(sqlContext.table(tableName)).nonEmpty
//
//  /** Registers the specified table in-memory catalog. */
//  def registerTable(tableName: String): Unit = registerSummaryDataset(sqlContext.table(tableName), Some(tableName))
//
//  /** Removes the specified table from the in-memory catalog. */
//  def unregisterTable(tableName: String): Unit = unregisterSummaryDataset(sqlContext.table(tableName))
//  
//  override def unregisterAllTables(): Unit = {
//    clearSummaryDatasetCatalog()
//  }

  /** Acquires a read lock on the catalog for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val lock = registerLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the catalog for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val lock = registerLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Clears all summary tables. */
  private[mv] def refresh(): Unit = writeLock {
    summaryDatasets.clear()
  }

  /** Checks if the catalog is empty. */
  private[mv] def isEmpty: Boolean = readLock {
    summaryDatasets.isEmpty
  }

  /**
   * Registers the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  private[mv] def registerSchema(dataMapSchema: DataMapSchema): Unit = writeLock {
    //TODO Add mvfunction here, don't use preagg function
    //    val updatedQuery = parser.addPreAggFunction(dataMapSchema.getCtasQuery)
    //    val query = sparkSession.sql(updatedQuery)
    val parser = new CarbonSpark2SqlParser
    val updatedQuery = parser.addPreAggFunction(dataMapSchema.getCtasQuery)
    val planToRegister = sparkSession.sql(updatedQuery).queryExecution.analyzed.
      transform {
        case p @ logical.Project(head :: tail, _) if head.isInstanceOf[Alias] && head.name.equals("preAgg") => p.copy(projectList = tail)
        case a @ logical.Aggregate(_, head :: tail, _) if head.isInstanceOf[Alias] && head.name.equals("preAgg") => a.copy(aggregateExpressions = tail)
      }
    if (lookupSummaryDataset(planToRegister).nonEmpty) {
      sys.error(s"Asked to register already registered.")
    } else {
      val modularPlan = modularizer.modularize(optimizer.execute(planToRegister)).next()
      val signature = modularPlan.signature
      val identifier = dataMapSchema.getRelationIdentifier
      val mvRepresentation = new FindDataSourceTable(sparkSession)(sparkSession.sessionState.catalog
        .lookupRelation(TableIdentifier(identifier.getTableName, Some(identifier.getDatabaseName))))

      summaryDatasets +=
        SummaryDataset(signature, planToRegister, dataMapSchema, mvRepresentation)
    }
  }

  /** Removes the given [[DataFrame]] from the catalog */
  private[mv] def unregisterSchema(dataMapName: String): Unit = writeLock {
      val dataIndex = summaryDatasets
        .indexWhere(sd => sd.dataMapSchema.getDataMapName.equals(dataMapName))
      require(dataIndex >= 0, s"Datamap $dataMapName is not registered.")
      summaryDatasets.remove(dataIndex)
  }
  
  def listAllSchema(): Array[SummaryDataset] = summaryDatasets.toArray
  
  /**
   * Registers the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  private[mv] def registerSummaryDataset(
      query: DataFrame, 
      tableName: Option[String] = None): Unit = writeLock {
    val planToRegister = query.queryExecution.analyzed
    if (lookupSummaryDataset(planToRegister).nonEmpty) {
      sys.error(s"Asked to register already registered.")
    } else {
      val modularPlan = modularizer.modularize(optimizer.execute(planToRegister)).next()//.harmonized
      val signature = modularPlan.signature
      summaryDatasets +=
        SummaryDataset(signature,planToRegister,null,null)
    }
  }

  /** Removes the given [[DataFrame]] from the catalog */
  private[mv] def unregisterSummaryDataset(query: DataFrame): Unit = writeLock {
    val planToRegister = query.queryExecution.analyzed
    val dataIndex = summaryDatasets.indexWhere(sd => planToRegister.sameResult(sd.plan))
    require(dataIndex >= 0, s"Table $query is not registered.")
    summaryDatasets.remove(dataIndex)
  }

  /** Tries to remove the data set for the given [[DataFrame]] from the catalog if it's registered */
  private[mv] def tryUnregisterSummaryDataset(
      query: DataFrame, 
      blocking: Boolean = true): Boolean = writeLock {
    val planToRegister = query.queryExecution.analyzed
    val dataIndex = summaryDatasets.indexWhere(sd => planToRegister.sameResult(sd.plan))
    val found = dataIndex >= 0
    if (found) {
      summaryDatasets.remove(dataIndex)
    }
    found
  }

  /** Optionally returns registered data set for the given [[DataFrame]] */
  private[mv] def lookupSummaryDataset(query: DataFrame): Option[SummaryDataset] = readLock {
    lookupSummaryDataset(query.queryExecution.analyzed)
  }

  /** Returns feasible registered summary data sets for processing the given ModularPlan. */
  private[mv] def lookupSummaryDataset(plan: LogicalPlan): Option[SummaryDataset] = readLock {
    summaryDatasets.find(sd => plan.sameResult(sd.plan))
  }

  /** Returns feasible registered summary data sets for processing the given ModularPlan. */
  private[mv] def lookupFeasibleSummaryDatasets(plan: ModularPlan): Seq[SummaryDataset] = readLock {
    val sig = plan.signature
    val statusDetails = DataMapStatusManager.getEnabledDataMapStatusDetails
    // Only select the enabled datamaps for the query.
    val enabledDataSets = summaryDatasets.filter { p =>
      statusDetails.exists(_.getDataMapName.equalsIgnoreCase(p.dataMapSchema.getDataMapName))
    }

    val feasible = enabledDataSets.filter { x =>
      (x.signature, sig) match {
        case (Some(sig1), Some(sig2)) => {
          if (sig1.groupby && sig2.groupby && sig1.datasets.subsetOf(sig2.datasets)) true
          else if (!sig1.groupby && !sig2.groupby && sig1.datasets.subsetOf(sig2.datasets)) true
          else false
        }
        case _ => false
      }
    }
    // heuristics: more tables involved in a summary data set => higher query reduction factor
    feasible.sortWith(_.signature.get.datasets.size > _.signature.get.datasets.size)
  }

  def useSummaryDataset(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown {
      case currentFragment =>
        lookupSummaryDataset(currentFragment)
          .map(_.mvRepresentation.transform{case l@LogicalRelation(relation,_,catalogTable) => {
            val rewrites = AttributeMap(currentFragment.output.zip(l.output))
            LogicalRelation(relation,currentFragment.output.map(a => AttributeReference(rewrites(a).name,a.dataType,a.nullable,a.metadata)(a.exprId)),catalogTable)}})
          .getOrElse(currentFragment)
    }

    newPlan transformAllExpressions {
      case s: SubqueryExpression => s.withNewPlan(useSummaryDataset(s.plan))
    }
  }
}
