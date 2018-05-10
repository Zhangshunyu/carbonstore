package org.apache.carbondata.mv.plans.util

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonRawValue
import com.google.common.base.Objects
//import scala.annotation.meta.getter
//import scala.annotation.meta.param
//import com.google.common.base.MoreObjects

class TableCluster @JsonCreator() (@JsonProperty("fact") @JsonRawValue fact: Set[String], @JsonProperty("dimension") @JsonRawValue dimension: Set[String]) {
  
//  @JsonProperty
  def getFact() = {
    fact
  }
//  
//  @JsonProperty
  def getDimension() = {
    dimension
  }

  @Override
  override def toString = {
    Objects.toStringHelper(this)
    .add("fact", fact)
    .add("dimension", dimension)
    .toString
  }
  
  /*
  @Override
  def toString = {
    MoreObjects.toStringHelper(this)
    .add("fact", fact)
    .add("dimension", dimension)
    .toString
  }
  * 
  */
}