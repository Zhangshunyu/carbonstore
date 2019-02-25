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

package org.apache.carbondata.core.indexstore;

import org.apache.carbondata.core.util.ByteUtil;

public class RangeColumnSplitMerger {

  private short[] primaryColIndexes;

  private byte[][] minValues;

  private byte[][] maxValues;

  public RangeColumnSplitMerger(short[] primaryColIndexes, byte[][] minValues, byte[][] maxValues) {
    this.primaryColIndexes = primaryColIndexes;
    this.minValues = minValues;
    this.maxValues = maxValues;
  }

  public boolean canBeMerged(RangeColumnSplitMerger other) {
    boolean selected = false;
    if (primaryColIndexes.length > 0) {
      for (int i = 0; i < primaryColIndexes.length; i++) {
        int minToMaxCompare = ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(minValues[primaryColIndexes[i]], other.maxValues[primaryColIndexes[i]]);

        int minToMinCompare = ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(minValues[primaryColIndexes[i]], other.minValues[primaryColIndexes[i]]);

        int maxToMinCompare = ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(maxValues[primaryColIndexes[i]], other.minValues[primaryColIndexes[i]]);

        if (minToMinCompare >= 0 && minToMaxCompare <= 0) {
          selected = true;
        } else if (minToMinCompare <= 0 && maxToMinCompare >= 0) {
          selected = true;
        } else {
          selected = false;
        }
      }
    }
    return selected;
  }

  public byte[][] getMinValues() {
    return minValues;
  }

  public byte[][] getMaxValues() {
    return maxValues;
  }
}
