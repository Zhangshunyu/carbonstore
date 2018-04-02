package org.apache.carbondata.mv.tool.vo;

import java.util.List;
import java.util.Map;

public class TableDetailsVo {

  private String tableName;

  private String createTableStmt;

  private Map<String, ColStats> colsStatsMap;

  private boolean isFactTable;

  private TblStatsVo tblStatsVo;

  public TableDetailsVo(String tableName, boolean isFactTable, String createTableStmt,
      TblStatsVo tblStatsVo, Map<String, ColStats> colsStatsMap) {
    this.tableName = tableName;
    this.createTableStmt = createTableStmt;
    this.colsStatsMap = colsStatsMap;
    this.isFactTable = isFactTable;
    this.tblStatsVo = tblStatsVo;
  }

  public String getTableName() {
    return tableName;
  }

  public String getCreateTableStmt() {
    return createTableStmt;
  }

  public Map<String, ColStats> getColumnStats() {
    return colsStatsMap;
  }

  public boolean isFactTable() {
    return isFactTable;
  }

  public TblStatsVo getTblStatsVo() {
    return tblStatsVo;
  }
}
