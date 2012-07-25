package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InvalidateBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockContext extends EntityContext<InvalidatedBlock> {

  private Map<InvalidatedBlock, InvalidatedBlock> invBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private Map<String, HashSet<InvalidatedBlock>> storageIdToInvBlocks = new HashMap<String, HashSet<InvalidatedBlock>>();
  private Map<InvalidatedBlock, InvalidatedBlock> newInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private Map<InvalidatedBlock, InvalidatedBlock> removedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private boolean allInvBlocksRead = false;
  private InvalidateBlockDataAccess dataAccess;

  public InvalidatedBlockContext(InvalidateBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(InvalidatedBlock invBlock) throws PersistanceException {
    if (removedInvBlocks.containsKey(invBlock)) {
      throw new TransactionContextException("Removed invalidated-block passed to be persisted");
    }

    if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
      storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
    } else {
      HashSet<InvalidatedBlock> invBlockList = new HashSet<InvalidatedBlock>();
      invBlockList.add(invBlock);
      storageIdToInvBlocks.put(invBlock.getStorageId(), invBlockList);
    }

    invBlocks.put(invBlock, invBlock);
    newInvBlocks.put(invBlock, invBlock);
    log("added-invblock", CacheHitState.NA, 
            new String[]{"bid", Long.toString(invBlock.getBlockId()), "sid", invBlock.getStorageId()});
  }

  @Override
  public void clear() {
    invBlocks.clear();
    storageIdToInvBlocks.clear();
    newInvBlocks.clear();
    removedInvBlocks.clear();
    allInvBlocksRead = false;
  }

  @Override
  public int count(CounterType<InvalidatedBlock> counter, Object... params) throws PersistanceException {
    InvalidatedBlock.Counter iCounter = (InvalidatedBlock.Counter) counter;
    switch (iCounter) {
      case All:
        log("count-all-invblocks");
        return dataAccess.countAll();
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public InvalidatedBlock find(FinderType<InvalidatedBlock> finder, Object... params) throws PersistanceException {
    InvalidatedBlock.Finder iFinder = (InvalidatedBlock.Finder) finder;

    switch (iFinder) {
      case ByPrimaryKey:
        long blockId = (Long) params[0];
        String storageId = (String) params[1];
        InvalidatedBlock searchInstance = new InvalidatedBlock(storageId, blockId);
        if (invBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return invBlocks.get(searchInstance);
        } else if (removedInvBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk-removed", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return null;
        } else {
          log("find-invblock-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          InvalidatedBlock result = dataAccess.findInvBlockByPkey(params);
          this.invBlocks.put(result, result);
          return result;
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public List<InvalidatedBlock> findList(FinderType<InvalidatedBlock> finder, Object... params) throws PersistanceException {
    InvalidatedBlock.Finder iFinder = (InvalidatedBlock.Finder) finder;

    switch (iFinder) {
      case ByStorageId:
        String storageId = (String) params[0];
        if (storageIdToInvBlocks.containsKey(storageId)) {
          log("find-invblocks-by-storageid", CacheHitState.HIT, new String[]{"sid", storageId});
          return new ArrayList<InvalidatedBlock>(this.storageIdToInvBlocks.get(storageId)); //clone the list reference
        } else {
          log("find-invblocks-by-storageid", CacheHitState.LOSS, new String[]{"sid", storageId});
          return syncInstances(dataAccess.findInvalidatedBlockByStorageId(storageId));
        }
      case All:
        if (!allInvBlocksRead) {
          log("find-all-invblocks", CacheHitState.LOSS);
          List<InvalidatedBlock> result = syncInstances(dataAccess.findAllInvalidatedBlocks());
          allInvBlocksRead = true;
          return result;
        } else {
          log("find-all-invblocks", CacheHitState.HIT);
          return new ArrayList<InvalidatedBlock>(invBlocks.values());
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedInvBlocks.values(), newInvBlocks.values(), null);
  }

  @Override
  public void remove(InvalidatedBlock invBlock) throws TransactionContextException {
    if (!invBlocks.containsKey(invBlock)) {
      throw new TransactionContextException("Unattached invalidated-block passed to be removed");
    }

    invBlocks.remove(invBlock);
    newInvBlocks.remove(invBlock);
    removedInvBlocks.put(invBlock, invBlock);
    if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
      HashSet<InvalidatedBlock> ibs = storageIdToInvBlocks.get(invBlock.getStorageId());
      ibs.remove(invBlock);
      if (ibs.isEmpty()) {
        storageIdToInvBlocks.remove(invBlock.getStorageId());
      }
    }
    log("removed-invblock", CacheHitState.NA, 
            new String[]{"bid", Long.toString(invBlock.getBlockId()), "sid", invBlock.getStorageId()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(InvalidatedBlock entity) throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private List<InvalidatedBlock> syncInstances(Collection<InvalidatedBlock> list) {
    List<InvalidatedBlock> finalList = new ArrayList<InvalidatedBlock>();
    for (InvalidatedBlock invBlock : list) {
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
          finalList.add(invBlocks.get(invBlock));
        } else {
          invBlocks.put(invBlock, invBlock);
          finalList.add(invBlock);
        }
        if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
          storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
        } else {
          HashSet<InvalidatedBlock> invBlockList = new HashSet<InvalidatedBlock>();
          invBlockList.add(invBlock);
          storageIdToInvBlocks.put(invBlock.getStorageId(), invBlockList);
        }
      }
    }

    return finalList;
  }
}
