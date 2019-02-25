package org.apache.spark.util

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.carbondata.core.indexstore.RangeColumnSplitMerger
import org.apache.carbondata.core.util.ByteUtil

class RangeColumnSplitMergerTest extends FunSuite {

  test("test" ) {

    val ranges = Seq((35,55),(25,35),(1, 40), (40, 50), (60, 120), (150, 220), (100, 145), (300, 400))

    val rangesBytes = ranges.map{case (m, x) =>
      (ByteUtil.toBytes(m), ByteUtil.toBytes(x))
    }

    val m = rangesBytes.map(_._1)
    val x = rangesBytes.map(_._2)

    val merger = rangesBytes.map{case (m, x) =>
      new RangeColumnSplitMerger(Array(0), Array(m), Array(x))
    }
    val groups = new ArrayBuffer[ArrayBuffer[RangeColumnSplitMerger]]()
    merger.foreach{s =>
      groups.find{f =>
        f.find(_.canBeMerged(s)) match {
          case Some(split) =>
            f += s
            true
          case _ => false
        }
      } match {
        case Some(p) =>
        case _ =>
          val splits = new ArrayBuffer[RangeColumnSplitMerger]()
          splits += s
          groups += splits
      }
    }

    groups.foreach{s =>
      val c = s.map{l =>
        (ByteUtil.toInt(l.getMinValues()(0), 0),ByteUtil.toInt(l.getMaxValues()(0), 0))
      }.toArray
      c.foreach(x => print(x))
      println("_________")
    }
  }

}
