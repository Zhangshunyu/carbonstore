package org.apache.carbondata.core.scan.primarykey;

import java.util.Comparator;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class PrimaryKeyVectorComparator implements Comparator<IteratorHolder> {

  private SerializableComparator[] comparators;

  private int[] sortOrdinals;

  public PrimaryKeyVectorComparator(DataType[] dataTypes, int[] sortOrdinals) {
    comparators = new SerializableComparator[dataTypes.length];
    this.sortOrdinals = sortOrdinals;
    for (int i = 0; i < dataTypes.length; i++) {
      comparators[i] =
          org.apache.carbondata.core.util.comparator.Comparator.getComparator(dataTypes[i]);
    }
  }

  @Override public int compare(IteratorHolder o1, IteratorHolder o2) {
    int diff = 0;
    for (int i = 0; i < sortOrdinals.length; i++) {
      diff = comparators[i].compare(o1.getCell(sortOrdinals[i]), o2.getCell(sortOrdinals[i]));
      if (diff != 0) {
        return diff;
      }
    }
    return diff;
  }

}
