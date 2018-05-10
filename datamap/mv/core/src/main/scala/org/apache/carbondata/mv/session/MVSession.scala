/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.session

import java.io.Closeable
//import com.huawei.mqo.plans.modular.ModularPatterns
//import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.carbondata.mv.plans.modular.ModularPlan
import java.util.concurrent.atomic.AtomicReference
import org.apache.carbondata.mv.rewrite.{SummaryDatasetCatalog,Navigator,DefaultMatchMaker,QueryRewrite}
import org.apache.carbondata.mv.session.internal.{SessionState, SharedState}
import scala.collection.mutable
import scala.util.{Try,Success,Failure}
import java.math.BigInteger
//import java.io.PrintWriter
//import java.net.URI
//import org.apache.hadoop.fs.Path
//import org.joda.time.DateTime
//import java.text.SimpleDateFormat
//import java.util.TimeZone

/**
 * The entry point for working with multi-query optimization in Sparky. Allow the 
 * creation of CSEs (covering subexpression) as well as query rewrite before
 * submitting to SparkSQL
 */
class MVSession private(
    @transient val sparkSession: SparkSession,
    @transient private val existingSharedState: Option[SharedState])
  extends Serializable with Closeable {  
  
  self =>
    
  private[mv] def this(ss: SparkSession) = {
    this(ss, None)
  }
  
  private[mv] def this(ss: SparkSession, catalog: SummaryDatasetCatalog) = {
    this(ss, Some(new SharedState(ss.sparkContext, Some(catalog))))
  }
  //spark.sparkContext.assertNotStopped()
  
  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */
  
  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   */
  private[mv] lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkSession.sparkContext))
  }
  
  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  private[mv] lazy val sessionState: SessionState = new SessionState(self)
//  private[sql] lazy val sessionState: SessionState = {
//    SparkSession.reflect[SessionState, SparkSession](
//      SparkSession.sessionStateClassName(sparkContext.conf),
//      self)
//  }
   
  @transient
  lazy val tableFrequencyMap = new mutable.HashMap[String,Int]
  
  @transient
  lazy val consumersMap = new mutable.HashMap[BigInteger, mutable.Set[LogicalPlan]] with mutable.MultiMap[BigInteger, LogicalPlan]
  
  def rewrite(analyzed: LogicalPlan): QueryRewrite = {
    sessionState.rewritePlan(analyzed)
  }

  def rewriteToSQL(analyzed: LogicalPlan): String = {
    val queryRewrite = rewrite(analyzed)
    Try(queryRewrite.withSummaryData) match {
      case Success(rewrittenPlan) => {
        if (rewrittenPlan.fastEquals(queryRewrite.modularPlan)) ""
        else {
          Try(rewrittenPlan.asCompactSQL) match {
            case Success(s) => s
            case Failure(e) => ""
          }
        }
      }
      case Failure(e) => ""
    }
  }
  
  override def close(): Unit = sparkSession.close()
  
}

/**
 * This MQOSession object contains utility functions to create a singleton MQOSession instance,
 * or to get the last created MQOSession instance.
 */

object MVSession {
//  
//  private val INSTANTIATION_LOCK = new Object()
//  
//  @transient private val lastInstantiatedContext = new AtomicReference[MQOContext]()
//  
//  def getOrCreate(sqlContext: SQLContext): MQOContext = {
//    INSTANTIATION_LOCK.synchronized {
//      if (lastInstantiatedContext.get() == null) {
//        new MQOContext(sqlContext)
//      }
//    }
//    lastInstantiatedContext.get()
//  }
//  
//  private[mqo] def clearLastInstantiatedContext(): Unit = {
//    INSTANTIATION_LOCK.synchronized {
//      lastInstantiatedContext.set(null)
//    }
//  }
//  
//  private[mqo] def setLastInstantiatedContext(mqoContext: MQOContext): Unit = {
//    INSTANTIATION_LOCK.synchronized {
//      lastInstantiatedContext.set(mqoContext)
//    }
//  }
}
