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

import java.io.Serializable;

public class PartitionMeta implements Serializable {

  private static final long serialVersionUID = 1L;

  private int index;

  private Object[] startKey;

  private Object[] endKey;

  private long size;

  public PartitionMeta(int index, Object[] startKey, Object[] endKey, long size) {
    this.index = index;
    this.startKey = startKey;
    this.endKey = endKey;
    this.size = size;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public Object[] getStartKey() {
    return startKey;
  }

  public void setStartKey(Object[] startKey) {
    this.startKey = startKey;
  }

  public Object[] getEndKey() {
    return endKey;
  }

  public void setEndKey(Object[] endKey) {
    this.endKey = endKey;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public void addSize(long size) {
    this.size = this.size + size;
  }
}
