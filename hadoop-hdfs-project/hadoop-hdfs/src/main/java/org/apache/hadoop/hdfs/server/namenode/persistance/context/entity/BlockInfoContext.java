package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class BlockInfoContext implements EntityContext<BlockInfo> {

  /**
   * block info table info
   */
  public static final String TABLE_NAME = "block_infos";
  public static final String BLOCK_ID = "block_id";
  public static final String BLOCK_INDEX = "block_index";
  public static final String INODE_ID = "inode_id";
  public static final String NUM_BYTES = "num_bytes";
  public static final String GENERATION_STAMP = "generation_stamp";
  public static final String BLOCK_UNDER_CONSTRUCTION_STATE = "block_under_construction_state";
  public static final String TIME_STAMP = "time_stamp";
  public static final String PRIMARY_NODE_INDEX = "primary_node_index";
  public static final String BLOCK_RECOVERY_ID = "block_recovery_id";
  /**
   * 
   */
  protected Map<Long, BlockInfo> blocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> modifiedBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> removedBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, List<BlockInfo>> inodeBlocks = new HashMap<Long, List<BlockInfo>>();
  protected boolean allBlocksRead = false;

  @Override
  public void clear() {
    blocks.clear();
    modifiedBlocks.clear();
    removedBlocks.clear();
    inodeBlocks.clear();
    allBlocksRead = false;
  }

  @Override
  public List<BlockInfo> findList(FinderType<BlockInfo> finder, Object... params) {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    List<BlockInfo> result = null;
    switch (bFinder) {
      case ByInodeId:
        long inodeId = (Long) params[0];
        if (inodeBlocks.containsKey(inodeId)) {
          result = inodeBlocks.get(inodeId);
        } else {
          result = findByInodeId(inodeId);
          inodeBlocks.put(inodeId, result);
        }
        break;

      case ByStorageId:
        String storageId = (String) params[0];
        result = findByStorageId(storageId);
        break;
      case All:
        if (allBlocksRead) {
          return new ArrayList<BlockInfo>(blocks.values());
        } else {
          result = findAllBlocks();
          allBlocksRead = true;
        }
        break;
    }

    return result;
  }

  @Override
  public BlockInfo find(FinderType<BlockInfo> finder, Object... params) {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    BlockInfo result = null;
    switch (bFinder) {
      case ById:
        long id = (Long) params[0];
        result = blocks.get(id);
        if (result == null) {
          result = findById(id);
          if (result != null)
            blocks.put(id, result);
        }
        break;
    }

    return result;
  }

  @Override
  public void update(BlockInfo block) throws TransactionContextException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    modifiedBlocks.put(block.getBlockId(), block);
  }

  @Override
  public void remove(BlockInfo block) throws TransactionContextException {
    if (block.getBlockId() == 0l) {
      throw new TransactionContextException("Unassigned-Id block passed to be removed");
    }

    BlockInfo attachedBlock = blocks.get(block.getBlockId());

    if (attachedBlock == null) {
      throw new TransactionContextException("Unattached block passed to be removed");
    }

    blocks.remove(block.getBlockId());
    modifiedBlocks.remove(block.getBlockId());
    removedBlocks.put(block.getBlockId(), attachedBlock);
  }

  @Override
  public void add(BlockInfo block) throws TransactionContextException {
    update(block);
  }

  protected abstract List<BlockInfo> findByStorageId(String storageId);

  protected abstract List<BlockInfo> findAllBlocks();

  protected abstract BlockInfo findById(long id);

  protected abstract List<BlockInfo> findByInodeId(long id);
  
}
