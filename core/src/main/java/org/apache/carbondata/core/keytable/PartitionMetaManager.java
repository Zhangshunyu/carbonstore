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

package org.apache.carbondata.core.keytable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.log4j.Logger;

public class PartitionMetaManager {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(PartitionMetaManager.class.getName());

  public static void updatePartitionMeta(
      String partitionMetaFilePath,
      PartitionMeta[] partitionMetas
  ) throws IOException {
    AtomicFileOperations fileWrite =
        AtomicFileOperationFactory.getAtomicFileOperations(partitionMetaFilePath);
    DataOutputStream dataOutput = null;
    ObjectOutputStream objectOutput = null;
    try {
      dataOutput = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      objectOutput = new ObjectOutputStream(dataOutput);
      objectOutput.writeObject(partitionMetas);
    } catch (IOException ioe) {
      LOGGER.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != objectOutput) {
        objectOutput.flush();
      }
      CarbonUtil.closeStreams(objectOutput, dataOutput);
      fileWrite.close();
    }
  }

  public static PartitionMeta[] readPartitionMeta(
      String partitionMetaFilePath
  ) throws IOException, ClassNotFoundException {
    DataInputStream dataInput = null;
    ObjectInputStream objectInput = null;
    PartitionMeta[] partitionMetas = null;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(partitionMetaFilePath);
    try {
      if (!FileFactory.isFileExist(
          partitionMetaFilePath, FileFactory.getFileType(partitionMetaFilePath))) {
        return new PartitionMeta[0];
      }
      dataInput = fileOperation.openForRead();
      objectInput = new ObjectInputStream(dataInput);
      partitionMetas = (PartitionMeta[]) objectInput.readObject();
    } catch (IOException e) {
      LOGGER.error("Failed to read metadata of load", e);
      throw e;
    } catch (ClassNotFoundException e) {
      throw e;
    } finally {
      CarbonUtil.closeStreams(objectInput, dataInput);
    }
    if (null == partitionMetas) {
      return new PartitionMeta[0];
    }
    return partitionMetas;
  }

}