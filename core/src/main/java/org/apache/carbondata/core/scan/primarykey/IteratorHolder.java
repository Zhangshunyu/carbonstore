package org.apache.carbondata.core.scan.primarykey;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;

public interface IteratorHolder extends Comparable<IteratorHolder> {

  boolean hasNext();

  void read();

  Object getCell(int columnOrdinal);

  BlockExecutionInfo getBlockExecutionInfo();

  boolean isDeleted();
}
