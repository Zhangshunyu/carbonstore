package org.apache.carbondata.core.scan.primarykey.merger;

import org.apache.carbondata.core.scan.primarykey.PrimaryKeyRowComparator;

public class PrimaryKeyMerger {

  private Object[] mergedKey;

  private PrimaryKeyRowComparator rowComparator;

  private int versionColIndex;

  private int deleteColIndex;

  private int[] colOrdinals;

  private int tupleIdIndex = -1;

  public PrimaryKeyMerger(PrimaryKeyRowComparator rowComparator, int versionColIndex,
      int deleteColIndex, int[] colOrdinals) {
    this.rowComparator = rowComparator;
    this.versionColIndex = versionColIndex;
    this.deleteColIndex = deleteColIndex;
    this.colOrdinals = colOrdinals;
  }

  public boolean mergeRow(Object[] row) {
    if (rowComparator.compare(mergedKey, row) == 0) {
      if ((long) row[versionColIndex] > (long) mergedKey[versionColIndex]) {
        merge(row, mergedKey);
      } else if ((long) row[versionColIndex] < (long) mergedKey[versionColIndex]){
        Object[] tmprow = new Object[row.length];
        System.arraycopy(row, 0, tmprow, 0, mergedKey.length);
        merge(mergedKey, tmprow);
        mergedKey = tmprow;
      } else {
        if ((long) mergedKey[deleteColIndex] > 0) {
          Object[] tmprow = new Object[row.length];
          System.arraycopy(row, 0, tmprow, 0, mergedKey.length);
          merge(mergedKey, tmprow);
          mergedKey = tmprow;
          mergedKey[deleteColIndex] = 0L;
        } else {
          merge(row, mergedKey);
        }
      }
      return true;
    }
    return false;
  }

  private void merge(Object[] row, Object[] mergedKey) {
    if ((long) row[deleteColIndex] > 0) {
      for (int i1 = 0; i1 < colOrdinals.length; i1++) {
        if (row[colOrdinals[i1]] != null) {
          mergedKey[colOrdinals[i1]] = null;
        }
      }
      mergedKey[deleteColIndex] = row[deleteColIndex];
      mergedKey[versionColIndex] = row[versionColIndex];
      if (tupleIdIndex > 0) {
        mergedKey[tupleIdIndex] = row[tupleIdIndex];
      }
    } else {
      for (int i1 = 0; i1 < mergedKey.length; i1++) {
        if (row[i1] != null) {
          mergedKey[i1] = row[i1];
        }
      }
    }
  }

  public void setTupleIdIndex(int tupleIdIndex) {
    this.tupleIdIndex = tupleIdIndex;
  }

  public boolean isDataAdded() {
    return mergedKey != null;
  }

  public void addFreshRow(Object[] row) {
    if (mergedKey == null) {
      mergedKey = new Object[row.length];
    }
    System.arraycopy(row, 0, mergedKey, 0, row.length);
  }

  public boolean isDeletedRow() {
    if (mergedKey == null) {
      return true;
    }
    if ((long)mergedKey[deleteColIndex] == 2) {
      return true;
    }
    if (colOrdinals.length == 0) {
      return false;
    }
    for (int i = 0; i < colOrdinals.length; i++) {
      if (mergedKey[colOrdinals[i]] != null) {
        return false;
      }
    }
    return true;
  }

  public Object[] getMergedRow() {
    return mergedKey;
  }
}
