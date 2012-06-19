package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class IndexedReplicaStorage implements Storage<IndexedReplica> {

  protected Map<String, IndexedReplica> modifiedReplicas = new HashMap<String, IndexedReplica>();
  protected Map<String, IndexedReplica> removedReplicas = new HashMap<String, IndexedReplica>();
  protected Map<Long, List<IndexedReplica>> blockReplicas = new HashMap<Long, List<IndexedReplica>>();

  @Override
  public void clear() {
    modifiedReplicas.clear();
    removedReplicas.clear();
    blockReplicas.clear();
  }
}
