package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 *
 * @author jude
 */
public class CorruptReplica extends Replica {

  public CorruptReplica(long blockId, String storageId) {
    super(storageId, blockId);
  }

  public String persistanceKey() {
    return blockId + storageId;
  }
}
