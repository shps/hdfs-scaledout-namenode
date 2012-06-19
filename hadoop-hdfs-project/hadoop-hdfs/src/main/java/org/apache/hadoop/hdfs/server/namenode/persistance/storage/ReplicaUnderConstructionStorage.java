package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class ReplicaUnderConstructionStorage implements Storage<ReplicaUnderConstruction> {

  protected Map<String, ReplicaUnderConstruction> modifiedReplicasUc = new HashMap<String, ReplicaUnderConstruction>();
  protected Map<String, ReplicaUnderConstruction> removedReplicasUc = new HashMap<String, ReplicaUnderConstruction>();
  protected Map<Long, List<ReplicaUnderConstruction>> blockReplicasUc = new HashMap<Long, List<ReplicaUnderConstruction>>();

  @Override
  public void clear() {
    modifiedReplicasUc.clear();
    removedReplicasUc.clear();
    blockReplicasUc.clear();
  }
}
