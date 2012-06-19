package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum PendingBlockFinder implements Finder<PendingBlockInfo> {

  ByPKey, All, ByTimeLimit;

  @Override
  public Class getType() {
    return PendingBlockInfo.class;
  }
}
