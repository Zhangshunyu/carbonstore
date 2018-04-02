package org.apache.carbondata.mv.tool.vo;

public class ColStats {

  private long distinctCount;

  private String maxValue;

  private String minValue;

  private int maxLength;

  private int nullCount;

  public ColStats(long distinctCount) {
    this.distinctCount = distinctCount;
  }

  public long getDistinctCount() {
    return distinctCount;
  }

  public String getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(String maxValue) {
    this.maxValue = maxValue;
  }

  public String getMinValue() {
    return minValue;
  }

  public void setMinValue(String minValue) {
    this.minValue = minValue;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public void setMaxLength(int maxLength) {
    this.maxLength = maxLength;
  }

  public int getNullCount() {
    return nullCount;
  }

  public void setNullCount(int nullCount) {
    this.nullCount = nullCount;
  }
}
