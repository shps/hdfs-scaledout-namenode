package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.avro.generic.GenericData.Array;
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
  private Map<Long, HashSet<InvalidatedBlock>> blockIdToInvBlocks = new HashMap<Long, HashSet<InvalidatedBlock>>();
  private Map<InvalidatedBlock, InvalidatedBlock> newInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private Map<InvalidatedBlock, InvalidatedBlock> removedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private boolean allInvBlocksRead = false;
  private int nullCount = 0;
  private InvalidateBlockDataAccess dataAccess;

  public InvalidatedBlockContext(InvalidateBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(InvalidatedBlock invBlock) throws PersistanceException {
    if (removedInvBlocks.containsKey(invBlock)) {
      throw new TransactionContextException("Removed invalidated-block passed to be persisted");
    }

    if (invBlocks.containsKey(invBlock) && invBlocks.get(invBlock) == null) {
      nullCount--;
    }

    invBlocks.put(invBlock, invBlock);
    newInvBlocks.put(invBlock, invBlock);

    if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
      storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
    }

    if (blockIdToInvBlocks.containsKey(invBlock.getBlockId())) {
      blockIdToInvBlocks.get(invBlock.getBlockId()).add(invBlock);
    }

    log("added-invblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(invBlock.getBlockId()), "sid", invBlock.getStorageId()});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    invBlocks.clear();
    storageIdToInvBlocks.clear();
    newInvBlocks.clear();
    removedInvBlocks.clear();
    blockIdToInvBlocks.clear();
    allInvBlocksRead = false;
    nullCount = 0;
  }

  @Override
  public int count(CounterType<InvalidatedBlock> counter, Object... params) throws PersistanceException {
    InvalidatedBlock.Counter iCounter = (InvalidatedBlock.Counter) counter;
    switch (iCounter) {
      case All:
        if (allInvBlocksRead) {
          log("count-all-invblocks", CacheHitState.HIT);
          return invBlocks.size() - nullCount;
        } else {
          log("count-all-invblocks", CacheHitState.LOSS);
          aboutToAccessStorage();
          return dataAccess.countAll();
        }
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
        if (blockIdToInvBlocks.containsKey(blockId)) { // if inv-blocks are queried by bid but the search-key deos not exist
          if (!blockIdToInvBlocks.get(blockId).contains(searchInstance)) {
            log("find-invblock-by-pk-not-exist", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
            return null;
          }
        }
        // otherwise search-key should be the new query or it must be a hit
        if (invBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return invBlocks.get(searchInstance);
        } else if (removedInvBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk-removed", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return null;
        } else {
          log("find-invblock-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          aboutToAccessStorage();
          InvalidatedBlock result = dataAccess.findInvBlockByPkey(params);
          if (result == null) {
            nullCount++;
          }
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
      case ByBlockId:
        long bid = (Long) params[0];
        if (blockIdToInvBlocks.containsKey(bid)) {
          log("find-invblocks-by-bid", CacheHitState.HIT, new String[]{"bid", String.valueOf(bid)});
          return new ArrayList<InvalidatedBlock>(this.blockIdToInvBlocks.get(bid)); //clone the list reference
        } else {
          log("find-invblocks-by-bid", CacheHitState.LOSS, new String[]{"bid", String.valueOf(bid)});
          aboutToAccessStorage();
          return syncInstancesForBlockId(dataAccess.findInvalidatedBlocksByBlockId(bid), bid);
        }
      case ByStorageId:
        String storageId = (String) params[0];
        if (storageIdToInvBlocks.containsKey(storageId)) {
          log("find-invblocks-by-storageid", CacheHitState.HIT, new String[]{"sid", storageId});
          return new ArrayList<InvalidatedBlock>(this.storageIdToInvBlocks.get(storageId)); //clone the list reference
        } else {
          log("find-invblocks-by-storageid", CacheHitState.LOSS, new String[]{"sid", storageId});
          aboutToAccessStorage();
          return syncInstancesForStorageId(dataAccess.findInvalidatedBlockByStorageId(storageId), storageId);
        }
      case All:
        List<InvalidatedBlock> result = new ArrayList<InvalidatedBlock>();
        if (!allInvBlocksRead) {
          log("find-all-invblocks", CacheHitState.LOSS);
          aboutToAccessStorage();
          syncInstances(dataAccess.findAllInvalidatedBlocks());
          allInvBlocksRead = true;
        } else {
          log("find-all-invblocks", CacheHitState.HIT);
        }
        for (InvalidatedBlock invBlk : invBlocks.values()) {
          if (invBlk != null) {
            result.add(invBlk);
          }
        }
        return result;
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
      // This is not necessary for invalidated-block
//      throw new TransactionContextException("Unattached invalidated-block passed to be removed");
    }

    invBlocks.remove(invBlock);
    newInvBlocks.remove(invBlock);
    removedInvBlocks.put(invBlock, invBlock);
    if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
      HashSet<InvalidatedBlock> ibs = storageIdToInvBlocks.get(invBlock.getStorageId());
      ibs.remove(invBlock);
    }
    if (blockIdToInvBlocks.containsKey(invBlock.getBlockId())) {
      HashSet<InvalidatedBlock> ibs = blockIdToInvBlocks.get(invBlock.getBlockId());
      ibs.remove(invBlock);
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

  /**
   *
   * @param list returns only the data fetched from the storage not those cached
   * in memory.
   * @return
   */
  private List<InvalidatedBlock> syncInstances(Collection<InvalidatedBlock> list) {
    List<InvalidatedBlock> finalList = new ArrayList<InvalidatedBlock>();
    for (InvalidatedBlock invBlock : list) {
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
          if (invBlocks.get(invBlock) == null) {
            invBlocks.put(invBlock, invBlock);
            nullCount--;
          }
          finalList.add(invBlocks.get(invBlock));
        } else {
          invBlocks.put(invBlock, invBlock);
          finalList.add(invBlock);
        }
      }
    }

    return finalList;
  }

  private List<InvalidatedBlock> syncInstancesForStorageId(Collection<InvalidatedBlock> list, String sid) {
    HashSet<InvalidatedBlock> ibs = new HashSet<InvalidatedBlock>();
    for (InvalidatedBlock newBlock : newInvBlocks.values()) {
      if (newBlock.getStorageId().equals(sid)) {
        ibs.add(newBlock);
      }
    }

    filterRemovedBlocks(list, ibs);
    storageIdToInvBlocks.put(sid, ibs);

    return new ArrayList<InvalidatedBlock>(ibs);
  }

  private void filterRemovedBlocks(Collection<InvalidatedBlock> list, HashSet<InvalidatedBlock> existings) {
    for (InvalidatedBlock invBlock : list) {
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
          existings.add(invBlocks.get(invBlock));
        } else {
          invBlocks.put(invBlock, invBlock);
          existings.add(invBlock);
        }
      }
    }
  }

  private List<InvalidatedBlock> syncInstancesForBlockId(Collection<InvalidatedBlock> list, long bid) {
    HashSet<InvalidatedBlock> ibs = new HashSet<InvalidatedBlock>();
    for (InvalidatedBlock newBlock : newInvBlocks.values()) {
      if (newBlock.getBlockId() == bid) {
        ibs.add(newBlock);
      }
    }

    filterRemovedBlocks(list, ibs);
    blockIdToInvBlocks.put(bid, ibs);

    return new ArrayList<InvalidatedBlock>(ibs);
  }
}
