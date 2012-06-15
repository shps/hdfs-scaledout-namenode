package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import se.sics.clusterj.ReplicaUcTable;

/**
 *
 * @author Kamal Hakimzadeh <kamal@sics.se>
 */
public class ReplicaUcFactory {

  public static List<ReplicaUnderConstruction> createReplicaList(List<ReplicaUcTable> replicaUc) {
    List<ReplicaUnderConstruction> replicas = new ArrayList<ReplicaUnderConstruction>(replicaUc.size());
    for (ReplicaUcTable t : replicaUc) {
      replicas.add(new ReplicaUnderConstruction(HdfsServerConstants.ReplicaState.values()[t.getState()], t.getStorageId(), t.getBlockId(), t.getIndex()));
    }
    return replicas;
  }

  public static void createPersistable(ReplicaUnderConstruction replica, ReplicaUcTable newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setState(replica.getState().ordinal());
  }
  
}
