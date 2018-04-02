package org.apache.carbondata.mv.tool.vo;

public class TblStatsVo {

  private long tableSize;

  private long rowCount;

  public TblStatsVo(long tableSize, long rowCount) {
    this.tableSize = tableSize;
    this.rowCount = rowCount;
  }

  public long getTableSize() {
    return tableSize;
  }

  public long getRowCount() {
    return rowCount;
  }
}
