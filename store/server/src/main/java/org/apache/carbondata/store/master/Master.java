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

package org.apache.carbondata.store.master;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.store.protocol.EchoRequest;
import org.apache.carbondata.store.protocol.EchoResponse;
import org.apache.carbondata.store.protocol.SearchRequest;
import org.apache.carbondata.store.protocol.SearchResult;
import org.apache.carbondata.store.protocol.WorkerGrpc;
import org.apache.carbondata.store.util.GrpcSerdes;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

public class Master {

  private static final LogService LOG = LogServiceFactory.getLogService(Master.class.getName());

  public static final String DRIVER_HOSTNAME = "localhost";

  public static final int DRIVER_PORT = 10020;

  private static Server registryServer;

  private int port;

  private List<WorkerGrpc.WorkerBlockingStub> searchers;

  public Master(int port) {
    this.port = port;
    this.searchers = new ArrayList<>();
  }

  public void startService() throws IOException {
    if (registryServer == null) {
      /* The port on which the registryServer should run */
      registryServer = ServerBuilder.forPort(port)
          .addService(new RegistryService(this))
          .build()
          .start();
      LOG.info("Master started, listening on " + port);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override public void run() {
          // Use stderr here since the logger may have been reset by its JVM shutdown hook.
          LOG.info("*** shutting down gRPC Master since JVM is shutting down");
          stopService();
          LOG.info("*** Master shut down");
        }
      });
    }
  }

  public void stopService() {
    if (registryServer != null) {
      registryServer.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (registryServer != null) {
      registryServer.awaitTermination();
    }
  }

  void registerSearcher(String searcherHostname, int port) {
    // A new searcher is trying to register, connect to this searcher
    LOG.info("trying to connect to searcher " + searcherHostname + ":" + port);
    ManagedChannel channel = ManagedChannelBuilder.forAddress(searcherHostname, port)
        .usePlaintext(true)
        .build();
    WorkerGrpc.WorkerBlockingStub blockingStub = WorkerGrpc.newBlockingStub(channel);
    tryEcho(blockingStub);
    searchers.add(blockingStub);
  }

  private void tryEcho(WorkerGrpc.WorkerBlockingStub stub) {
    EchoRequest request = EchoRequest.newBuilder().setMessage("hello").build();
    LOG.info("echo to searcher: " + request.getMessage());
    EchoResponse response = stub.echo(request);
    LOG.info("echo from searcher: " + response.getMessage());
  }

  public CarbonRow[] search(CarbonTable table, String[] columns, Expression filter)
      throws IOException, InvalidConfigurationException {
    Objects.requireNonNull(table);

    JobConf jobConf = new JobConf(new Configuration());
    Job job = new Job(jobConf);
    CarbonTableInputFormat<Object> format = CarbonInputFormatUtil.createCarbonTableInputFormat(
        job, table, columns, filter, null, null);

    SearchRequest.Builder builder = SearchRequest.newBuilder()
        .setQueryId(new Random().nextInt())
        .setTableInfo(GrpcSerdes.serialize(table.getTableInfo()));
    List<InputSplit> splits = format.getSplits(job);
    for (InputSplit split : splits) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DataOutput dataOutput = new DataOutputStream(stream);
      ((CarbonInputSplit)split).write(dataOutput);
      builder.addSplits(ByteString.copyFrom(stream.toByteArray()));
    }
    for (String column : columns) {
      builder.addProjectColumns(column);
    }
    if (filter != null) {
      builder.setFilterExpression(GrpcSerdes.serialize(filter));
    }
    SearchRequest request = builder.build();

    List<CarbonRow> output = new LinkedList<>();
    for (WorkerGrpc.WorkerBlockingStub searcher : searchers) {
      SearchResult result = searcher.search(request);
      collectResult(result, output);
    }
    return output.toArray(new CarbonRow[output.size()]);
  }

  private void collectResult(SearchResult result,  List<CarbonRow> output) throws IOException {
    for (ByteString bytes : result.getRowList()) {
      CarbonRow row = GrpcSerdes.deserialize(bytes);
      output.add(row);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Master master = new Master(DRIVER_PORT);
    master.startService();
    master.blockUntilShutdown();
  }
}
