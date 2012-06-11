package org.apache.hadoop.hdfs.server.namenode.persistance;

import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import se.sics.clusterj.ExcessReplicaTable;
import se.sics.clusterj.InvalidateBlocksTable;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaFactory {

  public static InvalidatedBlock createReplica(InvalidateBlocksTable invBlockTable) {
    return new InvalidatedBlock(invBlockTable.getStorageId(), invBlockTable.getBlockId(), 
            invBlockTable.getGenerationStamp(), invBlockTable.getNumBytes());
  }

  public static ExcessReplica createReplica(ExcessReplicaTable exReplicaTable) {
    return new ExcessReplica(exReplicaTable.getStorageId(), exReplicaTable.getBlockId());
  }

  public static void createPersistable(InvalidatedBlock invBlock, InvalidateBlocksTable newInvTable) {
    newInvTable.setBlockId(invBlock.getBlockId());
    newInvTable.setStorageId(invBlock.getStorageId());
    newInvTable.setGenerationStamp(invBlock.getGenerationStamp());
    newInvTable.setNumBytes(invBlock.getNumBytes());
  }

  public static void createPersistable(ExcessReplica exReplica, ExcessReplicaTable exReplicaTable) {
    exReplicaTable.setBlockId(exReplica.getBlockId());
    exReplicaTable.setStorageId(exReplica.getStorageId());
  }
}
