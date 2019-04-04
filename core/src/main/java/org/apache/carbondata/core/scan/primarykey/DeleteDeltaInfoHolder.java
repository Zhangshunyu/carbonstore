package org.apache.carbondata.core.scan.primarykey;

import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;

public class DeleteDeltaInfoHolder {

  private DeleteDeltaBlockDetails deleteDeltaBlockDetails;

  private BlockExecutionInfo blockExecutionInfo;

  private int deleteRowCount;

  public DeleteDeltaInfoHolder(DeleteDeltaBlockDetails deleteDeltaBlockDetails, BlockExecutionInfo blockExecutionInfo) {
    this.deleteDeltaBlockDetails = deleteDeltaBlockDetails;
    this.blockExecutionInfo = blockExecutionInfo;
  }

  public DeleteDeltaBlockDetails getDeleteDeltaBlockDetails() {
    return deleteDeltaBlockDetails;
  }

  public BlockExecutionInfo getBlockExecutionInfo() {
    return blockExecutionInfo;
  }

  public void incrementDeleteCounter() {
    deleteRowCount++;
  }

  public int getDeleteRowCount() {
    return deleteRowCount;
  }
}
