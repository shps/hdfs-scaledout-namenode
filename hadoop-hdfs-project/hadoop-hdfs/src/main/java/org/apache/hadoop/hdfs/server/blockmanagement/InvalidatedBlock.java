
package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlock extends Replica {

  public InvalidatedBlock(String storageId, long blockId) {
    super(storageId, blockId);
  }
}
