package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class PendingBlockStorage implements Storage<PendingBlockInfo> {

  protected Map<Long, PendingBlockInfo> pendings = new HashMap<Long, PendingBlockInfo>();
  protected Map<Long, PendingBlockInfo> modifiedPendings = new HashMap<Long, PendingBlockInfo>();
  protected Map<Long, PendingBlockInfo> removedPendings = new HashMap<Long, PendingBlockInfo>();
  protected boolean allPendingRead = false;

  @Override
  public void clear() {
    pendings.clear();
    modifiedPendings.clear();
    removedPendings.clear();
    allPendingRead = false;
  }
}
