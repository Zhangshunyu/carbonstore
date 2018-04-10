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

package org.apache.carbondata.datamap;

import org.apache.carbondata.core.datamap.DataMapProvider;
import org.apache.carbondata.core.datamap.IndexDataMapProvider;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.DiskBasedDMSchemaStorageProvider;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.spark.util.CarbonScalaUtil;

import static org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.MV;
import static org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.PREAGGREGATE;
import static org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES;

import org.apache.spark.sql.SparkSession;

public class DataMapManager {

  private static DataMapManager INSTANCE;

  private DataMapManager() { }

  public static synchronized DataMapManager get() {
    if (INSTANCE == null) {
      INSTANCE = new DataMapManager();
    }
    return INSTANCE;
  }

  /**
   * Return a DataMapClassProvider instance for specified dataMapSchema.
   */
  public DataMapProvider getDataMapProvider(String providerName,
      SparkSession sparkSession) {
    DataMapProvider provider;
    if (providerName.equalsIgnoreCase(PREAGGREGATE.toString())) {
      provider = new PreAggregateDataMapProvider(sparkSession);
    } else if (providerName.equalsIgnoreCase(TIMESERIES.toString())) {
      provider = new TimeseriesDataMapProvider(sparkSession);
    } else if (providerName.equalsIgnoreCase(MV.toString())) {
      provider = (DataMapProvider) CarbonScalaUtil
          .createDataMapProvider("org.apache.carbondata.mv.datamap.MVDataMapProvider", sparkSession,
              getDataMapSchemaStorageProvider());
    } else {
      provider = new IndexDataMapProvider(getDataMapSchemaStorageProvider());
    }
    return provider;
  }

  private DataMapSchemaStorageProvider getDataMapSchemaStorageProvider() {
    return new DiskBasedDMSchemaStorageProvider(
        CarbonProperties.getInstance().getSystemFolderLocation());
  }

}
