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

package org.apache.carbondata.service.scan;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.predict.PredictContext;

import org.apache.hadoop.mapreduce.RecordReader;

public class CarbonScan {

  private static LogService LOGGER = LogServiceFactory.getLogService(CarbonScan.class.getName());

  public static RecordReader<Void, Object> createRecordReader(CarbonMultiBlockSplit split,
      PredictContext context, CarbonTable carbonTable) throws VisionException {
    String selectId = context.getConf().selectId();
    String[] projection = context.getConf().projection();
    String[] projectionWithFeature = new String[projection.length + 1];
    System.arraycopy(projection,0, projectionWithFeature, 0, projection.length);
    projectionWithFeature[projection.length] = context.getConf().searchColumn();

    Object filter = context.getConf().conf(VisionConfiguration.SELECT_FILTER);

    QueryModel queryModel =
        new QueryModelBuilder(carbonTable)
            .projectColumns(projectionWithFeature)
            .filterExpression(filter == null ? null : (Expression) filter)
            .dataConverter(null)
            .build();
    queryModel.setVectorReader(false);
    queryModel.setForcedDetailRawQuery(false);
    queryModel.setRequiredRowId(false);
    if (context.getConf().searchVector() != null) {
      queryModel.setPredictContext(context);
    }

    int batchSize = context.getConf().batchSize();
    if (batchSize > 0) {
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE, "" + batchSize);
    }

    RecordReader<Void, Object> reader =
        new CarbonRecordReader<>(queryModel, new DictionaryDecodeReadSupport<Object>(), null);
    try {
      long t1 = System.currentTimeMillis();
      reader.initialize(split, null);
      long t2 = System.currentTimeMillis();
      LOGGER.audit("[" + selectId + "] CarbonScan initialize taken time: " + (t2 - t1) + " ms");
    } catch (InterruptedException e) {
      String message = "[" + selectId + "] Interrupted RecordReader initialization";
      LOGGER.error(e, message);
      throw new VisionException(message);
    } catch (IOException e) {
      String message = "[" + selectId + "] Failed to initialize RecordReader";
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
    return reader;
  }
}
