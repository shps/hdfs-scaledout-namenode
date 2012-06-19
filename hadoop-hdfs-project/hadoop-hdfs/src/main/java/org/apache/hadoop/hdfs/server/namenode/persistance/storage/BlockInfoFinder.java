
package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum BlockInfoFinder implements Finder<BlockInfo> {
  ById, ByInodeId, All, ByStorageId;

  @Override
  public Class getType() {
    return BlockInfo.class;
  }
}
