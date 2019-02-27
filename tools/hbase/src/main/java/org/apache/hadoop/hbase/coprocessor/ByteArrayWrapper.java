package org.apache.hadoop.hbase.coprocessor;

import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.hbase.util.Bytes;

public class ByteArrayWrapper {

  private byte[] binary;

  private int offset;

  private int len;

  public ByteArrayWrapper(byte[] binary, int offset, int len) {
    this.binary = binary;
    this.offset = offset;
    this.len = len;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ByteArrayWrapper that = (ByteArrayWrapper) o;
    return Bytes.equals(binary, offset, len, that.binary, that.offset, that.len);
  }

  @Override public int hashCode() {

    int result = Objects.hash(offset, len);
    result = 31 * result + Arrays.hashCode(binary);
    return result;
  }
}
