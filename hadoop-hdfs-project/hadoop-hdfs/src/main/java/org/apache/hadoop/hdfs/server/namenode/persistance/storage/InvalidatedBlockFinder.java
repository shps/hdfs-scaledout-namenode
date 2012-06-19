
package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum InvalidatedBlockFinder implements Finder<InvalidatedBlock> {
  ByStorageId, ByPrimaryKey, All;
  
  @Override
  public Class getType() {
    return InvalidatedBlock.class;
  }
  
}
