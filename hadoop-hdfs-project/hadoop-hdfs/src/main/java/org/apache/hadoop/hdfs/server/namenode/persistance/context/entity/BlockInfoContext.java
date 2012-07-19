package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoContext implements EntityContext<BlockInfo> {

  protected Map<Long, BlockInfo> blocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> newBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> modifiedBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> removedBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, List<BlockInfo>> inodeBlocks = new HashMap<Long, List<BlockInfo>>();
  protected boolean allBlocksRead = false;
  BlockInfoDataAccess dataAccess;

  public BlockInfoContext(BlockInfoDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(BlockInfo block) throws PersistanceException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    newBlocks.put(block.getBlockId(), block);
  }

  @Override
  public void clear() {
    blocks.clear();
    newBlocks.clear();
    modifiedBlocks.clear();
    removedBlocks.clear();
    inodeBlocks.clear();
    allBlocksRead = false;
  }

  @Override
  public int count(CounterType<BlockInfo> counter, Object... params) throws PersistanceException {
    BlockInfo.Counter bCounter = (BlockInfo.Counter) counter;
    switch (bCounter) {
      case All:
        if (allBlocksRead) {
          return blocks.size();
        } else {
          return dataAccess.countAll();
        }
    }
    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public BlockInfo find(FinderType<BlockInfo> finder, Object... params) throws PersistanceException {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    BlockInfo result = null;
    switch (bFinder) {
      case ById:
        long id = (Long) params[0];
        result = blocks.get(id);
        if (result == null) {
          result = dataAccess.findById(id);
          if (result != null) {
            blocks.put(id, result);
          }
        }
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public List<BlockInfo> findList(FinderType<BlockInfo> finder, Object... params) throws PersistanceException {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    List<BlockInfo> result = null;
    switch (bFinder) {
      case ByInodeId:
        long inodeId = (Long) params[0];
        if (inodeBlocks.containsKey(inodeId)) {
          return inodeBlocks.get(inodeId);
        } else {
          result = dataAccess.findByInodeId(inodeId);
          inodeBlocks.put(inodeId, syncBlockInfoInstances(result));
          return result;
        }

      case ByStorageId:
        String storageId = (String) params[0];
        result = dataAccess.findByStorageId(storageId);
        return syncBlockInfoInstances(result);
      case All:
        if (allBlocksRead) {
          return new ArrayList<BlockInfo>(blocks.values());
        } else {
          result = dataAccess.findAllBlocks();
          allBlocksRead = true;
          return syncBlockInfoInstances(result);
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedBlocks.values(), newBlocks.values(), modifiedBlocks.values());
  }

  @Override
  public void remove(BlockInfo block) throws PersistanceException {
    if (block.getBlockId() == 0l) {
      throw new TransactionContextException("Unassigned-Id block passed to be removed");
    }

    BlockInfo attachedBlock = blocks.get(block.getBlockId());

    if (attachedBlock == null) {
      throw new TransactionContextException("Unattached block passed to be removed");
    }

    blocks.remove(block.getBlockId());
    newBlocks.remove(block.getBlockId());
    modifiedBlocks.remove(block.getBlockId());
    removedBlocks.put(block.getBlockId(), attachedBlock);
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(BlockInfo block) throws PersistanceException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    modifiedBlocks.put(block.getBlockId(), block);
  }

  private List<BlockInfo> syncBlockInfoInstances(List<BlockInfo> newBlocks) {
    List<BlockInfo> finalList = new ArrayList<BlockInfo>();

    for (BlockInfo blockInfo : newBlocks) {
      if (blocks.containsKey(blockInfo.getBlockId()) && !removedBlocks.containsKey(blockInfo.getBlockId())) {
        finalList.add(blocks.get(blockInfo.getBlockId()));
      } else {
        blocks.put(blockInfo.getBlockId(), blockInfo);
        finalList.add(blockInfo);
      }
    }

    return finalList;
  }
}
