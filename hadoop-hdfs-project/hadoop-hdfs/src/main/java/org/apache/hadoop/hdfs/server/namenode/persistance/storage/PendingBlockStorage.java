package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class PendingBlockStorage implements Storage<PendingBlockInfo> {

  public static final String TABLE_NAME = "pending_blocks";
  public static final String BLOCK_ID = "block_id";
  public static final String TIME_STAMP = "time_stamp";
  public static final String NUM_REPLICAS_IN_PROGRESS = "num_replicas_in_progress";
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

  @Override
  public void remove(PendingBlockInfo pendingBlock) throws TransactionContextException {
    if (pendings.remove(pendingBlock.getBlockId()) == null) {
      throw new TransactionContextException("Unattached pending-block passed to be removed");
    }
    modifiedPendings.remove(pendingBlock.getBlockId());
    removedPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  @Override
  public List<PendingBlockInfo> findList(Finder<PendingBlockInfo> finder, Object... params) {
    PendingBlockFinder pFinder = (PendingBlockFinder) finder;
    List<PendingBlockInfo> result = null;
    switch (pFinder) {
      case ByTimeLimit:
        long timeLimit = (Long) params[0];
        result = findByTimeLimit(timeLimit);
        break;
      case All:
        if (allPendingRead) {
          result = new ArrayList(pendings.values());
        } else {
          result = findAll();
          allPendingRead = true;
        }
        break;
    }

    return result;
  }

  @Override
  public PendingBlockInfo find(Finder<PendingBlockInfo> finder, Object... params) {
    PendingBlockFinder pFinder = (PendingBlockFinder) finder;
    PendingBlockInfo result = null;
    switch (pFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        if (this.pendings.containsKey(blockId)) {
          return this.pendings.get(blockId);
        } else if (!this.removedPendings.containsKey(blockId)) {
          result = findByPKey(blockId);
          if (result != null) {
            this.pendings.put(blockId, result);
          }
        }
        break;
    }

    return result;
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(PendingBlockInfo pendingBlock) throws TransactionContextException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    modifiedPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  @Override
  public void add(PendingBlockInfo entity) throws TransactionContextException {
    update(entity);
  }

  protected abstract List<PendingBlockInfo> findByTimeLimit(long timeLimit);

  protected abstract List<PendingBlockInfo> findAll();

  protected abstract PendingBlockInfo findByPKey(long blockId);
}
