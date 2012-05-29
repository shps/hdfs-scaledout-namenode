package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.Replica;
import se.sics.clusterj.TripletsTable;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class ReplicaFactory {

  public static List<Replica> createReplicaList(List<TripletsTable> triplets) {
    List<Replica> replicas = new ArrayList<Replica>(triplets.size());
    for (TripletsTable t : triplets) {
      replicas.add(new Replica(t.getBlockId(), t.getStorageId(), t.getIndex()));
    }
    return replicas;
  }

  public static void createPersistable(Replica replica, TripletsTable newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
  }
  
}
