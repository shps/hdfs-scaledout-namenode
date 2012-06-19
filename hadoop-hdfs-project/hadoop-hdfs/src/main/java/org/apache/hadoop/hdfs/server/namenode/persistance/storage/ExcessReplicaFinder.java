
package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum ExcessReplicaFinder implements Finder<ExcessReplica> {
  ByStorageId, ByPKey;
  
  @Override
  public Class getType() {
    return ExcessReplica.class;
  }
  
}
