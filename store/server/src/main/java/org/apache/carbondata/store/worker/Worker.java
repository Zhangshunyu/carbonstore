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

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.store.master.Master;
import org.apache.carbondata.store.protocol.MasterGrpc;
import org.apache.carbondata.store.protocol.RegisterWorkerRequest;
import org.apache.carbondata.store.protocol.RegisterWorkerResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;

public class Worker {

  private static final LogService LOG = LogServiceFactory.getLogService(Worker.class.getName());

  public static final int SEARCHER_PORT = 10021;

  private ManagedChannel driverChannel;

  private Server server;

  private static boolean registered = false;

  private static Worker INSTANCE;

  public static Worker getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new Worker();
    }
    return INSTANCE;
  }

  private Worker() {
  }

  public void startService(int port) throws IOException {
    if (server == null) {
      /* The port on which the SearchService should run */
      server = ServerBuilder.forPort(port)
          .addService(new SearchService(10))
          .build()
          .start();
      LOG.info("Worker started, listening on " + port);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          // Use stderr here since the LOG may have been reset by its JVM shutdown hook.
          LOG.info("*** shutting down gRPC Worker since JVM is shutting down");
          stopService();
          LOG.info("*** Worker shut down");
        }
      });
    }
  }

  public void stopService() {
    if (server != null) {
      server.shutdown();
    }
  }

  public void registerToDriver(String driverHost, int port) throws IOException {
    if (registered) {
      return;
    }

    LOG.info("Registering to driver " + driverHost + ":" + port);
    // Construct client connecting to Master server at {@code host:port}.
    this.driverChannel = ManagedChannelBuilder.forAddress(driverHost, port)
        .usePlaintext(true)
        .build();
    MasterGrpc.MasterBlockingStub blockingStub = MasterGrpc.newBlockingStub(driverChannel);
    int cores = Runtime.getRuntime().availableProcessors();
    String searcherHostname = InetAddress.getLocalHost().getHostName();
    RegisterWorkerRequest request =
        RegisterWorkerRequest.newBuilder()
            .setHostname(searcherHostname)
            .setPort(SEARCHER_PORT)
            .setCores(cores)
            .build();
    RegisterWorkerResponse response;
    try {
      response = blockingStub.registerWorker(request);
    } catch (StatusRuntimeException e) {
      LOG.error(e, "RPC failed: " + e.getStatus());
      return;
    }
    registered = true;
    LOG.info("Register response from master: " + response.getMessage());
  }

  public void shutdown() throws InterruptedException {
    driverChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  public static boolean isRegistered() {
    return registered;
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Worker worker = new Worker();
    worker.registerToDriver(Master.DRIVER_HOSTNAME, Master.DRIVER_PORT);
    worker.startService(Worker.SEARCHER_PORT);
    worker.blockUntilShutdown();
  }

}
