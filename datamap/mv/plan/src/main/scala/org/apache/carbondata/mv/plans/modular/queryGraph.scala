/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans.modular

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.annotation.DeveloperApi
//import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class JoinEdge(left: Int, right: Int, joinType: JoinType)
/**
 * :: DeveloperApi ::
 */


// Map[Int, Vector[Int]] withDefaultValue Vector.empty