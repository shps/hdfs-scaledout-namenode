
package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import se.sics.clusterj.UnderReplicaBlocksTable;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class UnderReplicatedBlockFactory {
  public static void createPersistable(UnderReplicatedBlock block, UnderReplicaBlocksTable persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setLevel(block.getLevel());
  }

  public static UnderReplicatedBlock createUrBlock(UnderReplicaBlocksTable bit) {
    UnderReplicatedBlock block = new UnderReplicatedBlock(bit.getLevel(), bit.getBlockId());
    return block;
  }

  public static List<UnderReplicatedBlock> createUrBlockList(List<UnderReplicaBlocksTable> bitList) {
    List<UnderReplicatedBlock> blocks = new ArrayList<UnderReplicatedBlock>();
    for (UnderReplicaBlocksTable bit : bitList) {
      blocks.add(createUrBlock(bit));
    }
    return blocks;
  }
  
}
