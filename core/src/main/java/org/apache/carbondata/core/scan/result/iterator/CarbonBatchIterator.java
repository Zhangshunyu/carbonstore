package org.apache.carbondata.core.scan.result.iterator;

import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;

public interface CarbonBatchIterator {

  boolean hasNext();

  void processNextBatch(CarbonColumnarBatch columnarBatch);

  void close();
}
