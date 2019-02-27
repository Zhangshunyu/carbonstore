package org.apache.hadoop.hbase.coprocessor;

import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.hbase.util.Bytes;

public class QualifierArray {

  private byte[] cfBytes;

  private byte[] qualBytes;

  private int cfOffset;

  private int cfLen;

  private int qualOffset;

  private int qualLen;

  public QualifierArray(byte[] cfBytes, int cfOffset, int cfLen,byte[] qualBytes,
      int qualOffset, int qualLen) {
    this.cfBytes = cfBytes;
    this.cfOffset = cfOffset;
    this.cfLen = cfLen;
    this.qualBytes = qualBytes;
    this.qualOffset = qualOffset;
    this.qualLen = qualLen;
  }

  public QualifierArray() {
  }

  public void set(byte[] cfBytes, int cfOffset, int cfLen,byte[] qualBytes,
      int qualOffset, int qualLen) {
    this.cfBytes = cfBytes;
    this.cfOffset = cfOffset;
    this.cfLen = cfLen;
    this.qualBytes = qualBytes;
    this.qualOffset = qualOffset;
    this.qualLen = qualLen;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QualifierArray that = (QualifierArray) o;

    return Bytes.equals(cfBytes, cfOffset, cfLen, that.cfBytes, that.cfOffset, that.cfLen)
        && Bytes
        .equals(qualBytes, qualOffset, qualLen, that.qualBytes, that.qualOffset, that.qualLen);
  }

  @Override public int hashCode() {
    int result = Bytes.hashCode(cfBytes, cfOffset, cfLen);
    result = 31 * result + Bytes.hashCode(qualBytes, qualOffset, qualLen);
    return result;
  }


}
