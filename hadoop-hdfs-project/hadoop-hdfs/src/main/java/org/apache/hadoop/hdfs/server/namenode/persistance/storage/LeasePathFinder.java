
package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum LeasePathFinder implements Finder<LeasePath> {
  ByHolderId, ByPKey, ByPrefix, All;
  
  @Override
  public Class getType() {
    return LeasePath.class;
  }
  
}
