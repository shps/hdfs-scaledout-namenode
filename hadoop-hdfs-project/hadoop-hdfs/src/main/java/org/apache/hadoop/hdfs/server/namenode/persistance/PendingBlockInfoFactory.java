package org.apache.hadoop.hdfs.server.namenode.persistance;

import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import se.sics.clusterj.PendingReplicationBlockTable;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockInfoFactory {

  public static PendingBlockInfo createPendingBlockInfo(PendingReplicationBlockTable pendingTable) {
    return new PendingBlockInfo(pendingTable.getBlockId(),
            pendingTable.getTimestamp(), pendingTable.getNumReplicasInProgress());
  }

  public static void createPersistablePendingBlockInfo(PendingBlockInfo pendingBlock, PendingReplicationBlockTable pendingTable) {
    pendingTable.setBlockId(pendingBlock.getBlockId());
    pendingTable.setNumReplicasInProgress(pendingBlock.getNumReplicas());
    pendingTable.setTimestamp(pendingBlock.getTimeStamp());
  }
}
