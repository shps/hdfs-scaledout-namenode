package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplica extends Replica {

  public ExcessReplica(String storageId, long blockId) {
    super(storageId, blockId);
  }
}
