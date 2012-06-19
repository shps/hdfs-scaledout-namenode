
package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum IndexedReplicaFinder implements Finder<IndexedReplica> {
  ByBlockId;
  
  @Override
  public Class getType() {
    return IndexedReplica.class;
  }
  
}
