package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class BlockInfoStorage implements Storage<BlockInfo> {

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
  public List<BlockInfo> findList(Finder<BlockInfo> finder, Object... params) {
    BlockInfoFinder bFinder = (BlockInfoFinder) finder;
    List<BlockInfo> result = null;
    switch (bFinder) {
      case ByInodeId:
        long inodeId = (Long) params[0];
        if (inodeBlocks.containsKey(inodeId)) {
          result = inodeBlocks.get(inodeId);
        } else {
          result = findByInodeId(inodeId);
        }
        break;

      case ByStorageId:
        String storageId = (String) params[0];
        result = findByStorageId(storageId);
        break;
      case All:
        result = findAllBlocks();
    }

    return result;
  }

  protected abstract List<BlockInfo> findByInodeId(long id);

  protected List<BlockInfo> syncBlockInfoInstances(List<BlockInfo> newBlocks) {
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

  protected abstract List<BlockInfo> findByStorageId(String storageId);

  protected abstract List<BlockInfo> findAllBlocks();
}
