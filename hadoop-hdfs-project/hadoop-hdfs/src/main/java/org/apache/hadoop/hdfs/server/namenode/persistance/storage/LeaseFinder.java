package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum LeaseFinder implements Finder<Lease> {

  ByPKey, ByHolderId, All, ByTimeLimit;

  @Override
  public Class getType() {
    return Lease.class;
  }
}
