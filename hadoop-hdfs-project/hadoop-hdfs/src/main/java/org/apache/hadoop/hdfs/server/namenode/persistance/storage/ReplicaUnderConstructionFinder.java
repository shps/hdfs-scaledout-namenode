package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum ReplicaUnderConstructionFinder implements Finder<ReplicaUnderConstruction> {

  ByBlockId;

  @Override
  public Class getType() {
    return ReplicaUnderConstruction.class;
  }
}
