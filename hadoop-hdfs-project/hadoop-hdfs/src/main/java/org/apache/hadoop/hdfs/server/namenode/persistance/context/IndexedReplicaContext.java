package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class IndexedReplicaContext implements EntityContext<IndexedReplica> {

  public static final String TABLE_NAME = "indexed_replicas";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";
  public static final String REPLICA_INDEX = "replica_index";
  /**
   * Mappings
   */
  protected Map<String, IndexedReplica> removedReplicas = new HashMap<String, IndexedReplica>();
  protected Map<String, IndexedReplica> modifiedReplicas = new HashMap<String, IndexedReplica>();
  protected Map<Long, List<IndexedReplica>> blockReplicas = new HashMap<Long, List<IndexedReplica>>();

  @Override
  public void clear() {
    modifiedReplicas.clear();
    removedReplicas.clear();
    blockReplicas.clear();
  }

  @Override
  public void remove(IndexedReplica replica) throws TransactionContextException {
    modifiedReplicas.remove(replica.cacheKey());
    blockReplicas.get(replica.getBlockId()).remove(replica);
    removedReplicas.put(replica.cacheKey(), replica);
  }

  @Override
  public List<IndexedReplica> findList(FinderType<IndexedReplica> finder, Object... params) {
    IndexedReplica.Finder iFinder = (IndexedReplica.Finder) finder;
    List<IndexedReplica> result = null;

    switch (iFinder) {
      case ByBlockId:
        long id = (Long) params[0];
        if (blockReplicas.containsKey(id)) {
          result = blockReplicas.get(id);
        } else {
          result = findReplicasById(id);
          blockReplicas.put(id, result);
        }
        break;
    }
    
    if (result != null)
      return new ArrayList<IndexedReplica>(result);

    return result;
  }

  @Override
  public IndexedReplica find(FinderType<IndexedReplica> finder, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int count(CounterType counter, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(IndexedReplica replica) throws TransactionContextException {
    if (removedReplicas.containsKey(replica.cacheKey())) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    modifiedReplicas.put(replica.cacheKey(), replica);
  }

  @Override
  public void add(IndexedReplica replica) throws TransactionContextException {
    update(replica);
  }

  protected abstract List<IndexedReplica> findReplicasById(long id);
}
