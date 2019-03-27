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

package org.apache.spark.keytable

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partitioner

import org.apache.carbondata.core.keytable.PartitionMeta
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

object PartitionerMerger {

  def generatePartitioner(
      table: CarbonTable,
      oldParts: Array[PartitionMeta],
      bounds: Array[Array[AnyRef]],
      splitSize: Long,
      maxSplitSize: Long,
      rowOrdering: Ordering[Array[AnyRef]]
  ): StreamPartitioner = {
    var partBounds =
      oldParts.isEmpty match {
        case true =>
          generateByStreamingBounds(
            bounds,
            splitSize)
        case false =>
          bounds.isEmpty match {
            case true =>
              generateByPartitionMetas(
                oldParts,
                maxSplitSize,
                true)
            case false =>
              mergePartitionAndBound(
                oldParts,
                bounds,
                splitSize,
                maxSplitSize,
                rowOrdering)
          }
      }

    // update index and sync to old partitions
    partBounds = partBounds
      .zipWithIndex
      .map { entry =>
        if (entry._1.index != -1) {
          oldParts(entry._1.index).setIndex(entry._2)
        }
        entry._1.index = entry._2
        entry._1
      }

    new StreamPartitioner(partBounds, rowOrdering)
  }

  private def mergePartitionAndBound(
      oldParts: Array[PartitionMeta],
      bounds: Array[Array[AnyRef]],
      splitSize: Long,
      maxSplitSize: Long,
      rowOrdering: Ordering[Array[AnyRef]]
  ): Array[PartitionBound] = {
    val tempPartBounds =
      generateByPartitionMetas(
        oldParts,
        maxSplitSize,
        false)
    // map bounds to each PartitionBound
    val numParts = tempPartBounds.length
    val comparators = tempPartBounds.map(new PartitionBoundComparator(_, rowOrdering))
    val boundGroups = new Array[ArrayBuffer[Array[AnyRef]]](numParts)
    (0 until numParts).foreach { pIndex =>
      boundGroups(pIndex) = new ArrayBuffer[Array[AnyRef]](bounds.length)
    }
    (0 until bounds.length).foreach { bIndex =>
      var pIndex = 0
      while (pIndex < numParts && comparators(pIndex).gt(bounds(bIndex))) {
        pIndex += 1
      }
      boundGroups(pIndex) += bounds(bIndex)
    }
    // generate final PartitionBound list
    val partBounds = new ArrayBuffer[PartitionBound](numParts + bounds.length)
    // separate first PartitionBound
    separateIntervalByBounds(
      tempPartBounds(0),
      boundGroups(0),
      splitSize,
      rowOrdering,
      partBounds)
    // separate middle PartitionBounds
    (1 until numParts - 2).foreach { pIndex =>
      if (tempPartBounds(pIndex).index == -1) {
        // interval partition
        separateIntervalByBounds(
          tempPartBounds(pIndex),
          boundGroups(pIndex),
          splitSize,
          rowOrdering,
          partBounds)
      } else {
        // old partition
        if (boundGroups(pIndex + 1).isEmpty) {
          // merge interval
          partBounds += PartitionBound(tempPartBounds(pIndex).index,
            tempPartBounds(pIndex).startKey, tempPartBounds(pIndex).includeStartKey, false,
            tempPartBounds(pIndex + 1).endKey, tempPartBounds(pIndex + 1).includeEndKey, false,
            tempPartBounds(pIndex).size)
        } else {
          // use old partitionBounds directly
          partBounds += tempPartBounds(pIndex)
        }
      }
    }
    // separate last two PartitionBounds
    partBounds += tempPartBounds(numParts - 2)
    separateIntervalByBounds(
      tempPartBounds(numParts - 1),
      boundGroups(numParts - 1),
      splitSize,
      rowOrdering,
      partBounds)
    partBounds.toArray
  }

  private def separateIntervalByBounds(
      part: PartitionBound,
      bounds: ArrayBuffer[Array[AnyRef]],
      splitSize: Long,
      rowOrdering: Ordering[Array[AnyRef]],
      finalPartBounds: ArrayBuffer[PartitionBound]
  ): Unit = {
    val numBounds = bounds.length
    if (numBounds == 0) {
      if (part.toMax || part.toMin) {
        finalPartBounds += part
      }
    } else {
      finalPartBounds += PartitionBound(-1,
        part.startKey, part.includeStartKey, part.toMin,
        bounds(0), false, false,
        splitSize)
      (0 until numBounds - 1).foreach { bIndex =>
        finalPartBounds += PartitionBound(-1,
          bounds(bIndex), true, false,
          bounds(bIndex + 1), false, false,
          splitSize)
      }
      finalPartBounds += PartitionBound(-1,
        bounds(numBounds - 1), true, false,
        part.endKey, part.includeEndKey, part.toMax,
        splitSize)
    }
  }

  private def generateByPartitionMetas(oldParts: Array[PartitionMeta],
      maxSplitSize: Long,
      mergeSmall: Boolean
  ): Array[PartitionBound] = {
    val partBounds = new ArrayBuffer[PartitionBound](oldParts.length * 2 + 1)
    // if streaming bounds is empty, use old PartitionMetas to generate new PartitionMetas
    partBounds += PartitionBound(-1,
      null, false, true,
      oldParts(0).getStartKey, false, false,
      -1)
    val numParts = oldParts.length
    (0 until (numParts - 1)).foreach { pIndex =>
      if (mergeSmall && oldParts(pIndex).getSize < maxSplitSize) {
        partBounds += PartitionBound(oldParts(pIndex).getIndex,
          oldParts(pIndex).getStartKey, true, false,
          oldParts(pIndex + 1).getStartKey, false, false,
          oldParts(pIndex).getSize)
      } else {
        partBounds += PartitionBound(oldParts(pIndex).getIndex,
          oldParts(pIndex).getStartKey, true, false,
          oldParts(pIndex).getEndKey, true, false,
          oldParts(pIndex).getSize)
        partBounds += PartitionBound(-1,
          oldParts(pIndex).getEndKey, false, false,
          oldParts(pIndex + 1).getStartKey, false, false,
          -1)
      }
    }
    partBounds += PartitionBound(oldParts(numParts - 1).getIndex,
      oldParts(numParts - 1).getStartKey, true, false,
      oldParts(numParts - 1).getEndKey, true, false,
      oldParts(numParts - 1).getSize)
    partBounds += PartitionBound(-1,
      oldParts(numParts - 1).getEndKey, false, false,
      null, false, true,
      -1)
    partBounds.toArray
  }

  private def generateByStreamingBounds(
      bounds: Array[Array[AnyRef]],
      splitSize: Long
  ): Array[PartitionBound] = {
    val partitionBounds = new ArrayBuffer[PartitionBound](bounds.length + 1)
    // if not exist PartitionMeta, use streaming bounds to generate PartitionMeta directly
    if (bounds.isEmpty) {
      partitionBounds += PartitionBound(-1,
        null, false, true,
        null, false, true,
        -1)
    } else {
      partitionBounds += PartitionBound(-1,
        null, false, true,
        bounds(0), false, false,
        splitSize)
      (0 until (bounds.length - 1)).foreach { idx =>
        partitionBounds += PartitionBound(-1,
          bounds(idx), true, false,
          bounds(idx + 1), false, false,
          splitSize)
      }
      partitionBounds += PartitionBound(-1,
        bounds(partitionBounds.length - 1), true, false,
        null, false, true,
        splitSize)
    }
    partitionBounds.toArray
  }
}

class StreamPartitioner(
    val partitionBounds: Array[PartitionBound],
    rowOrdering: Ordering[Array[AnyRef]]
) extends Partitioner {

  def numPartitions: Int = partitionBounds.length

  var isFirst = true
  var comparators: Array[PartitionBoundComparator] = null

  def init(): Unit = {
    comparators = partitionBounds
      .map { bound =>
        new PartitionBoundComparator(bound, rowOrdering)
      }
  }

  def getPartition(key: Any): Int = {
    if (isFirst) {
      isFirst = false
      init()
    }
    val value = key.asInstanceOf[Array[AnyRef]]
    var partition = partitionBounds.length - 1
    while (partition > -1 && comparators(partition).lt(value)) {
      partition -= 1
    }
    partition
  }
}

case class PartitionBound(
    var index: Int,
    startKey: Array[AnyRef],
    includeStartKey: Boolean,
    toMin: Boolean,
    endKey: Array[AnyRef],
    includeEndKey: Boolean,
    toMax: Boolean,
    size: Long
)

class PartitionBoundComparator(
    partBound: PartitionBound,
    rowOrdering: Ordering[Array[AnyRef]]
) {
  val startKeyComparator = if (partBound.toMin) {
    (_: Array[AnyRef]) => true
  } else if (partBound.includeStartKey) {
    (value: Array[AnyRef]) => rowOrdering.gteq(value, partBound.startKey)
  } else {
    (value: Array[AnyRef]) => rowOrdering.gt(value, partBound.startKey)
  }

  val endKeyComparator = if (partBound.toMax) {
    (_: Array[AnyRef]) => true
  } else if (partBound.includeEndKey) {
    (value: Array[AnyRef]) => rowOrdering.lteq(value, partBound.endKey)
  } else {
    (value: Array[AnyRef]) => rowOrdering.lt(value, partBound.endKey)
  }

  def compare(value: Array[AnyRef]): Int = {
    if (endKeyComparator(value)) {
      if (startKeyComparator(value)) {
        0
      } else {
        -1
      }
    } else {
      1
    }
  }

  def gt(value: Array[AnyRef]): Boolean = {
    !endKeyComparator(value)
  }

  def lt(value: Array[AnyRef]): Boolean = {
    !startKeyComparator(value)
  }
}
