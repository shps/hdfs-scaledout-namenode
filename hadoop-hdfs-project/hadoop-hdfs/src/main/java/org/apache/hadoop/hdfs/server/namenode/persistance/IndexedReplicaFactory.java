package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.TripletsTable;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class IndexedReplicaFactory {

  public static List<IndexedReplica> createReplicaList(List<TripletsTable> triplets) {
    List<IndexedReplica> replicas = new ArrayList<IndexedReplica>(triplets.size());
    for (TripletsTable t : triplets) {
      replicas.add(new IndexedReplica(t.getBlockId(), t.getStorageId(), t.getIndex()));
    }
    return replicas;
  }

  public static void createPersistable(IndexedReplica replica, TripletsTable newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
  }
  
}
