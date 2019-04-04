package org.apache.carbondata.core.scan.primarykey;

import java.util.Iterator;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;

public class IteratorRowHolder implements IteratorHolder {

  private Object[] currentRow;

  private Iterator<Object[]> iterator;

  private PrimaryKeyVectorComparator comparator;

  private BlockExecutionInfo executionInfo;

  private int deletedRows;

  public IteratorRowHolder(PrimaryKeyVectorComparator comparator, Iterator<Object[]> iterator,
      BlockExecutionInfo executionInfo) {
    this.iterator = iterator;
    this.comparator = comparator;
    this.executionInfo = executionInfo;
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public void read() {
    currentRow = iterator.next();
  }

  @Override public int compareTo(IteratorHolder o) {
    return comparator.compare(this, o);
  }

  @Override public Object getCell(int columnOrdinal) {
    return currentRow[columnOrdinal];
  }

  public Object[] getCurrentRow() {
    return currentRow;
  }

  @Override public BlockExecutionInfo getBlockExecutionInfo() {
    return executionInfo;
  }

  @Override public boolean isDeleted() {
    return false;
  }
}
