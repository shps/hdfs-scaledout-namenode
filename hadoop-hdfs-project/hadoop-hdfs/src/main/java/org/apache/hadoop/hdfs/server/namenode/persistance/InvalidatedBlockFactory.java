package org.apache.hadoop.hdfs.server.namenode.persistance;

import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import se.sics.clusterj.InvalidateBlocksTable;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockFactory {

  public static InvalidatedBlock createInvalidatedBlock(InvalidateBlocksTable invBlockTable) {
    return new InvalidatedBlock(invBlockTable.getStorageId(), invBlockTable.getBlockId());
  }
}
