package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.PendingBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockContext implements EntityContext<PendingBlockInfo> {

  private Map<Long, PendingBlockInfo> pendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> newPendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> modifiedPendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> removedPendings = new HashMap<Long, PendingBlockInfo>();
  private boolean allPendingRead = false;
  private PendingBlockDataAccess dataAccess;

  public PendingBlockContext(PendingBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    newPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  @Override
  public void clear() {
    pendings.clear();
    newPendings.clear();
    modifiedPendings.clear();
    removedPendings.clear();
    allPendingRead = false;
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<PendingBlockInfo> findList(FinderType<PendingBlockInfo> finder, Object... params) throws PersistanceException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    List<PendingBlockInfo> result = null;
    switch (pFinder) {
      case ByTimeLimit:
        long timeLimit = (Long) params[0];
        return syncInstances(dataAccess.findByTimeLimit(timeLimit));
      case All:
        if (allPendingRead) {
          result = new ArrayList(pendings.values());
        } else {
          result = syncInstances(dataAccess.findAll());
          allPendingRead = true;
        }
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public PendingBlockInfo find(FinderType<PendingBlockInfo> finder, Object... params) throws PersistanceException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    PendingBlockInfo result = null;
    switch (pFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        if (this.pendings.containsKey(blockId)) {
          result = this.pendings.get(blockId);
        } else if (!this.removedPendings.containsKey(blockId)) {
          result = dataAccess.findByPKey(blockId);
          if (result != null) {
            this.pendings.put(blockId, result);
          }
        }
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void remove(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (pendings.remove(pendingBlock.getBlockId()) == null) {
      throw new TransactionContextException("Unattached pending-block passed to be removed");
    }
    newPendings.remove(pendingBlock.getBlockId());
    modifiedPendings.remove(pendingBlock.getBlockId());
    removedPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    modifiedPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  private List<PendingBlockInfo> syncInstances(Collection<PendingBlockInfo> pendingTables) {
    List<PendingBlockInfo> newPBlocks = new ArrayList<PendingBlockInfo>();
    for (PendingBlockInfo p : pendingTables) {
      if (pendings.containsKey(p.getBlockId())) {
        newPBlocks.add(pendings.get(p.getBlockId()));
      } else if (!removedPendings.containsKey(p.getBlockId())) {
        pendings.put(p.getBlockId(), p);
        newPBlocks.add(p);
      }
    }

    return newPBlocks;
  }
}
