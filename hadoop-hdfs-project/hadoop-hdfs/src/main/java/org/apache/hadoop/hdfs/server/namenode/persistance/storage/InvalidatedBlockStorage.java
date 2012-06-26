package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class InvalidatedBlockStorage implements Storage<InvalidatedBlock> {
  
  public static final String TABLE_NAME = "invalidated_blocks";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";
  public static final String GENERATION_STAMP = "generation_stamp";
  public static final String NUM_BYTES = "num_bytes";

  protected Map<InvalidatedBlock, InvalidatedBlock> invBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  protected Map<String, HashSet<InvalidatedBlock>> storageIdToInvBlocks = new HashMap<String, HashSet<InvalidatedBlock>>();
  protected Map<InvalidatedBlock, InvalidatedBlock> newInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  protected Map<InvalidatedBlock, InvalidatedBlock> removedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  protected boolean allInvBlocksRead = false;

  @Override
  public void clear() {
    invBlocks.clear();
    storageIdToInvBlocks.clear();
    newInvBlocks.clear();
    removedInvBlocks.clear();
    allInvBlocksRead = false;
  }

  @Override
  public void update(InvalidatedBlock entity) throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void add(InvalidatedBlock invBlock) throws TransactionContextException {
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
  }

  @Override
  public List<InvalidatedBlock> findList(Finder<InvalidatedBlock> finder, Object... params) {
    InvalidatedBlockFinder iFinder = (InvalidatedBlockFinder) finder;
    List<InvalidatedBlock> result = null;

    switch (iFinder) {
      case ByStorageId:
        String storageId = (String) params[0];
        if (storageIdToInvBlocks.containsKey(storageId)) {
          result = new ArrayList<InvalidatedBlock>(this.storageIdToInvBlocks.get(storageId)); //clone the list reference
        } else {
          result = findInvalidatedBlockByStorageId(storageId);
        }
        break;
      case All:
        if (!allInvBlocksRead) {
          result = findAllInvalidatedBlocks();
          allInvBlocksRead = true;
        } else {
          result = new ArrayList<InvalidatedBlock>(invBlocks.values());
        }
        break;
    }

    return result;
  }

  @Override
  public InvalidatedBlock find(Finder<InvalidatedBlock> finder, Object... params) {
    InvalidatedBlockFinder iFinder = (InvalidatedBlockFinder) finder;
    InvalidatedBlock result = null;

    switch (iFinder) {
      case ByPrimaryKey:
        long blockId = (Long) params[0];
        String storageId = (String) params[1];
        InvalidatedBlock searchInstance = new InvalidatedBlock(storageId, blockId);
        if (invBlocks.containsKey(searchInstance)) {
          result = invBlocks.get(searchInstance);
        } else if (removedInvBlocks.containsKey(searchInstance)) {
          result = null;
        } else {
          result = findInvBlockByPkey(params);
          this.invBlocks.put(result, result);
        }
        break;
    }
    return result;
  }

  protected abstract List<InvalidatedBlock> findInvalidatedBlockByStorageId(String storageId);

  protected abstract List<InvalidatedBlock> findAllInvalidatedBlocks();

  protected abstract InvalidatedBlock findInvBlockByPkey(Object[] params);
}
