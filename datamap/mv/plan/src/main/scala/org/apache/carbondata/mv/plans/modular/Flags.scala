package org.apache.carbondata.mv.plans.modular


trait Flags {
  // each query allows only one of each of the following keywords
  final val DISTINCT      = 1L << 0
  final val LIMIT         = 1L << 1
  final val SORT          = 1L << 2
  final val GLOBAL        = 1L << 3
  final val LOCAL         = 1L << 4
  final val EXPAND        = 1L << 5

  // to determine each Seq[Expression] in varagrgs belong to which keyword
  //  final val SortLimit = SORT | LIMIT 
  
  def flagToString(flag: Long): String = flag match {
    case      DISTINCT => "DISTINCT"
    case         LIMIT => "LIMIT"
  }
  
  // List of the raw flags that have expressions as arguments
  // TODO: add EXPAND
  private def pickledWithExpressions = Array[Long](SORT, LIMIT, EXPAND)
  
  final val MaxBitPosition = 6
  
  final val pickledListOrder: List[Long] = {
    val all = 0 to MaxBitPosition map (1L << _)
    all.toList filter (pickledWithExpressions contains _)
  }
  final val rawFlagPickledOrder: Array[Long] = pickledListOrder.toArray

  type FlagSet = Long
  
  val NoFlags: FlagSet = 0L
  
  implicit class FlagSetUtils(var flags: FlagSet) {
    def hasFlag(mask: Long) = (flags & mask) != 0L  
    def hasFlag(mask: Int): Boolean = hasFlag(mask.toLong)
    def setFlag(mask: Long) = { flags |= mask; flags }
    def resetFlag(mask: Long) = { flags &= ~mask; flags }
    def initFlags(mask: Long) = { flags = mask; flags }
  }
}

object Flags extends Flags