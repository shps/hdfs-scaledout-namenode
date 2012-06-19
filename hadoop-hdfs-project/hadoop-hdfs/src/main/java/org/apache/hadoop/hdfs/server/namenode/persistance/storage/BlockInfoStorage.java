package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.namenode.persistance.storage.Storage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

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
}
