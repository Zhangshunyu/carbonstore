package org.apache.carbondata.core.scan.primarykey;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.iterator.CarbonBatchIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;

public class IteratorVectorHolder implements IteratorHolder {

  private CarbonColumnarBatch columnarBatch;

  private PrimaryKeyVectorComparator comparator;

  private CarbonBatchIterator iterator;

  private int counter = -1;

  private boolean firstTme;

  private BlockExecutionInfo executionInfo;

  private int deletedRows;

  public IteratorVectorHolder(PrimaryKeyVectorComparator comparator, CarbonBatchIterator iterator,
      CarbonColumnarBatch columnarBatch, BlockExecutionInfo executionInfo) {
    this.iterator = iterator;
    this.columnarBatch = columnarBatch;
    this.comparator = comparator;
    this.executionInfo = executionInfo;
  }

  public boolean hasNext() {

    if (!firstTme) {
      firstTme = true;
      if (!processData()) {
        return false;
      }
    }
    if (counter >= columnarBatch.getActualSize() - 1 && iterator.hasNext()) {
      counter = -1;
      return processData();
    }
    return counter < columnarBatch.getActualSize() - 1 || iterator.hasNext();
  }

  private boolean processData() {
    boolean hasData = false;
    while (iterator.hasNext()) {
      columnarBatch.reset();
      iterator.processNextBatch(columnarBatch);
      if (columnarBatch.getActualSize() > 0) {
        hasData = true;
        break;
      }
    }
    return hasData;
  }

  public void read() {
    counter++;
  }

  @Override public Object getCell(int columnOrdinal) {
    return columnarBatch.columnVectors[columnOrdinal].getData(counter);
  }

  @Override public int compareTo(IteratorHolder o) {
    return comparator.compare(this, o);
  }

  public CarbonColumnarBatch getColumnarBatch() {
    return columnarBatch;
  }

  @Override public BlockExecutionInfo getBlockExecutionInfo() {
    return executionInfo;
  }

  @Override public void incrementDeleteRow() {
    deletedRows += 1;
  }

  @Override public int getDeleteRowCount() {
    return deletedRows;
  }

  @Override public boolean isDeleted() {
    return columnarBatch.getFilteredRows()[counter];
  }
}
