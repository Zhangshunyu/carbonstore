package org.apache.carbondata.mv.plans.util

import java.util.concurrent.atomic.AtomicLong

class SQLBuild private (
    nextSubqueryId: AtomicLong,
    subqueryPrefix: String) {
  self =>
  
  def this(subqueryPrefix: String) =
    this(new AtomicLong(0), subqueryPrefix)
    
  def newSubqueryName(): String = s"${subqueryPrefix}${nextSubqueryId.getAndIncrement()}"
}