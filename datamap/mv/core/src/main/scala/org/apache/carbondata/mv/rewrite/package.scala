/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

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

package org.apache.carbondata.mv

import org.apache.spark.annotation.DeveloperApi
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.carbondata.mv.plans.util.CheckSPJG
import org.apache.carbondata.mv.plans.util.Signature
import org.apache.carbondata.mv.plans.util.LogicalPlanSignatureGenerator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.execution.QueryExecution

/**
 * A a collection of common abstractions for query execution. 
 */
package object rewrite {

  implicit class QueryExecutionUtils(val qe: QueryExecution) {
    def withCachedAndMVData(catalog: SummaryDatasetCatalog): LogicalPlan = {
      val plan = qe.withCachedData
      catalog.useSummaryDataset(plan)
    }
  }
}
