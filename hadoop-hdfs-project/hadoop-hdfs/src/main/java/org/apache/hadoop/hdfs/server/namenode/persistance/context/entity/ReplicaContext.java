package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaContext extends EntityContext<IndexedReplica> {

  /**
   * Mappings
   */
  private Map<String, IndexedReplica> removedReplicas = new HashMap<String, IndexedReplica>();
  private Map<String, IndexedReplica> newReplicas = new HashMap<String, IndexedReplica>();
  private Map<String, IndexedReplica> modifiedReplicas = new HashMap<String, IndexedReplica>();
  private Map<Long, List<IndexedReplica>> blockReplicas = new HashMap<Long, List<IndexedReplica>>();
  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(IndexedReplica replica) throws PersistanceException {
    if (removedReplicas.containsKey(replica.cacheKey())) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    newReplicas.put(replica.cacheKey(), replica);
    log("added-replica", CacheHitState.NA, 
            new String[]{"bid", Long.toString(replica.getBlockId()),
              "sid", replica.getStorageId(), "index", Integer.toString(replica.getIndex())});
  }

  @Override
  public void clear() {
    modifiedReplicas.clear();
    removedReplicas.clear();
    blockReplicas.clear();
    super.clear();
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedReplicas.values(), newReplicas.values(), modifiedReplicas.values());
    log("prepared");
  }

  @Override
  public void remove(IndexedReplica replica) throws PersistanceException {
    modifiedReplicas.remove(replica.cacheKey());
    blockReplicas.get(replica.getBlockId()).remove(replica);
    removedReplicas.put(replica.cacheKey(), replica);
    log("removed-replica", CacheHitState.NA, 
            new String[]{"bid", Long.toString(replica.getBlockId()),
              "sid", replica.getStorageId(), "index", Integer.toString(replica.getIndex())});
  }

  @Override
  public IndexedReplica find(FinderType<IndexedReplica> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<IndexedReplica> findList(FinderType<IndexedReplica> finder, Object... params) throws PersistanceException {
    IndexedReplica.Finder iFinder = (IndexedReplica.Finder) finder;
    List<IndexedReplica> result = null;

    switch (iFinder) {
      case ByBlockId:
        long id = (Long) params[0];
        if (blockReplicas.containsKey(id)) {
          log("find-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(id)});
          result = blockReplicas.get(id);
        } else {
          log("find-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(id)});
          result = dataAccess.findReplicasById(id);
          blockReplicas.put(id, result);
        }
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(IndexedReplica replica) throws PersistanceException {
    if (removedReplicas.containsKey(replica.cacheKey())) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    modifiedReplicas.put(replica.cacheKey(), replica);
    log("updated-replica", CacheHitState.NA, 
            new String[]{"bid", Long.toString(replica.getBlockId()),
              "sid", replica.getStorageId(), "index", Integer.toString(replica.getIndex())});
  }
}
