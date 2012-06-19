package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class InvalidatedBlockStorage implements Storage<InvalidatedBlock> {

  protected Map<InvalidatedBlock, InvalidatedBlock> invBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  protected Map<String, HashSet<InvalidatedBlock>> storageIdToInvBlocks = new HashMap<String, HashSet<InvalidatedBlock>>();
  protected Map<InvalidatedBlock, InvalidatedBlock> modifiedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  protected Map<InvalidatedBlock, InvalidatedBlock> removedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  protected boolean allInvBlocksRead = false;

  @Override
  public void clear() {
    invBlocks.clear();
    storageIdToInvBlocks.clear();
    modifiedInvBlocks.clear();
    removedInvBlocks.clear();
    allInvBlocksRead = false;
  }
}
