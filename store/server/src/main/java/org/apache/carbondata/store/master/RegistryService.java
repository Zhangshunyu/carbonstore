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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.store.protocol.MasterGrpc;
import org.apache.carbondata.store.protocol.RegisterWorkerRequest;
import org.apache.carbondata.store.protocol.RegisterWorkerResponse;

import io.grpc.stub.StreamObserver;

class RegistryService extends MasterGrpc.MasterImplBase {
  private static final LogService LOG =
      LogServiceFactory.getLogService(RegistryService.class.getName());

  private Master master;

  public RegistryService(Master master) {
    this.master = master;
  }

  @Override public void registerWorker(
      RegisterWorkerRequest req,
      StreamObserver<RegisterWorkerResponse> responseObserver) {
    LOG.info(
        "RegisterWorkerRequest from " + req.getHostname() + " with " + req.getCores() + " cores");
    master.registerSearcher(req.getHostname(), req.getPort());
    RegisterWorkerResponse reply =
        RegisterWorkerResponse.newBuilder().setMessage("Ack: " + req.getHostname()).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
