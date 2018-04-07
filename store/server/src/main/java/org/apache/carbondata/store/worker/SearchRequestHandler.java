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

package org.apache.carbondata.store.worker;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.apache.carbondata.store.protocol.SearchRequest;
import org.apache.carbondata.store.protocol.SearchResult;
import org.apache.carbondata.store.util.GrpcSerdes;

import com.google.protobuf.ByteString;

class SearchRequestHandler implements Runnable {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SearchRequestHandler.class.getName());
  private boolean running = true;
  private Queue<SearchService.SearchRequestContext> requestQueue;

  SearchRequestHandler(Queue<SearchService.SearchRequestContext> requestQueue) {
    this.requestQueue = requestQueue;
  }

  public void run() {
    while (running) {
      SearchService.SearchRequestContext requestContext = requestQueue.poll();
      if (requestContext == null) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      } else {
        try {
          List<CarbonRow> rows = handleRequest(requestContext);
          sendSuccessResponse(requestContext, rows);
        } catch (IOException | InterruptedException e) {
          LOG.error(e);
          sendFailureResponse(requestContext, e);
        }
      }
    }
  }

  public void stop() {
    running = false;
  }

  private List<CarbonRow> handleRequest(SearchService.SearchRequestContext requestContext)
      throws IOException, InterruptedException {
    SearchRequest request = requestContext.getRequest();
    TableInfo tableInfo = GrpcSerdes.deserialize(request.getTableInfo());
    CarbonTable table = CarbonTable.buildFromTableInfo(tableInfo);
    Expression filter = GrpcSerdes.deserialize(request.getFilterExpression());
    String[] projectColumns = new String[request.getProjectColumnsCount()];
    for (int i = 0; i < request.getProjectColumnsCount(); i++) {
      projectColumns[i] = request.getProjectColumns(i);
    }

    QueryModel queryModel = new QueryModelBuilder(table)
        .projectColumns(projectColumns)
        .filterExpression(filter)
        .build();

    ByteString bs = request.getSplits(0);
    ByteArrayInputStream stream = new ByteArrayInputStream(bs.toByteArray());
    DataInputStream inputStream = new DataInputStream(stream);
    CarbonInputSplit split = new CarbonInputSplit();
    split.readFields(inputStream);

    CarbonRecordReader<Object[]> reader =
        new CarbonRecordReader<>(queryModel, new DictionaryDecodeReadSupport<Object[]>());
    reader.initialize(split, null);
    return readAllRows(reader);
  }

  private List<CarbonRow> readAllRows(CarbonRecordReader<Object[]> reader) throws IOException {
    List<CarbonRow> rows = new LinkedList<>();
    try {
      while (reader.nextKeyValue()) {
        rows.add(new CarbonRow(reader.getCurrentValue()));
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return rows;
  }

  private void sendFailureResponse(
      SearchService.SearchRequestContext requestContext,
      Throwable throwable) {
    SearchResult response = SearchResult.newBuilder()
        .setQueryId(requestContext.getRequest().getQueryId())
        .setStatus(SearchResult.Status.FAILURE)
        .setMessage(throwable.getMessage())
        .build();
    requestContext.getResponseObserver().onNext(response);
    requestContext.getResponseObserver().onCompleted();
  }

  private void sendSuccessResponse(
      SearchService.SearchRequestContext requestContext,
      List<CarbonRow> rows) throws IOException {
    SearchResult.Builder builder = SearchResult.newBuilder()
        .setQueryId(requestContext.getRequest().getQueryId())
        .setStatus(SearchResult.Status.SUCCESS)
        .setMessage("SUCCESS");
    for (CarbonRow row : rows) {
      builder.addRow(GrpcSerdes.serialize(row));
    }
    SearchResult response = builder.build();
    requestContext.getResponseObserver().onNext(response);
    requestContext.getResponseObserver().onCompleted();
  }
}
