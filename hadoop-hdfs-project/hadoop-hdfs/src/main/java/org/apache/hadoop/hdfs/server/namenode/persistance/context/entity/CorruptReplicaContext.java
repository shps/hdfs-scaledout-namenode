package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.CorruptReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class CorruptReplicaContext extends EntityContext<CorruptReplica> {

  protected Map<String, CorruptReplica> corruptReplicas = new HashMap<String, CorruptReplica>();
  protected Map<Long, List<CorruptReplica>> blockCorruptReplicas = new HashMap<Long, List<CorruptReplica>>();
  protected Map<String, CorruptReplica> newCorruptReplicas = new HashMap<String, CorruptReplica>();
  protected Map<String, CorruptReplica> modifiedCorruptReplicas = new HashMap<String, CorruptReplica>();
  protected Map<String, CorruptReplica> removedCorruptReplicas = new HashMap<String, CorruptReplica>();
  protected boolean allCorruptBlocksRead = false;
  private CorruptReplicaDataAccess dataAccess;

  public CorruptReplicaContext(CorruptReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(CorruptReplica entity) throws PersistanceException {
    if (removedCorruptReplicas.get(entity.persistanceKey()) != null) {
      throw new TransactionContextException("Removed corrupt replica passed to be persisted");
    }
    corruptReplicas.put(entity.persistanceKey(), entity);
    newCorruptReplicas.put(entity.persistanceKey(), entity);
    log("added-corrupt", CacheHitState.NA, 
            new String[]{"bid", Long.toString(entity.getBlockId()),"sid", entity.getStorageId()});
  }

  @Override
  public void clear() {
    corruptReplicas.clear();
    blockCorruptReplicas.clear();
    newCorruptReplicas.clear();
    modifiedCorruptReplicas.clear();
    removedCorruptReplicas.clear();
    allCorruptBlocksRead = false;
  }

  @Override
  public int count(CounterType<CorruptReplica> counter, Object... params) throws PersistanceException {
    CorruptReplica.Counter cCounter = (CorruptReplica.Counter) counter;

    switch (cCounter) {
      case All:
        if (allCorruptBlocksRead) {
          log("count-all-corrupts", CacheHitState.HIT);
          return corruptReplicas.size();
        } else {
          log("count-all-corrupts", CacheHitState.LOSS);
          return dataAccess.countAll();
        }
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public CorruptReplica find(FinderType<CorruptReplica> finder, Object... params) throws PersistanceException {
    CorruptReplica.Finder cFinder = (CorruptReplica.Finder) finder;

    switch (cFinder) {
      case ByPk:
        Long blockId = (Long) params[0];
        String storageId = (String) params[1];
        if (corruptReplicas.containsKey(blockId + storageId)) {
          log("find-corrupt-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return corruptReplicas.get(blockId + storageId);
        } else {
          log("find-corrupt-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return dataAccess.findByPk(blockId, storageId);
        }
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<CorruptReplica> findList(FinderType<CorruptReplica> finder, Object... params) throws PersistanceException {
    CorruptReplica.Finder cFinder = (CorruptReplica.Finder) finder;

    switch (cFinder) {
      case All:
        if (allCorruptBlocksRead) {
          log("find-all-corrupts", CacheHitState.HIT);
          return corruptReplicas.values();
        }
        log("find-all-corrupts", CacheHitState.LOSS);
        List<CorruptReplica> sync = syncCorruptReplicaInstances(dataAccess.findAll());
        allCorruptBlocksRead = true;
        return sync;
      case ByBlockId:
        Long blockId = (Long) params[0];
        if (blockCorruptReplicas.containsKey(blockId)) {
          log("find-corrupts-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId)});
          return blockCorruptReplicas.get(blockId);
        }
        log("find-corrupts-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId)});
        List<CorruptReplica> syncList = syncCorruptReplicaInstances(dataAccess.findByBlockId(blockId));
        blockCorruptReplicas.put(blockId, syncList);
        return syncList;

    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedCorruptReplicas.values(), newCorruptReplicas.values(), modifiedCorruptReplicas.values());
  }

  @Override
  public void remove(CorruptReplica entity) throws PersistanceException {
    if (corruptReplicas.get(entity.persistanceKey()) == null) {
      throw new TransactionContextException("Unattached corrupt replica passed to be removed");
    }

    corruptReplicas.remove(entity.persistanceKey());
    newCorruptReplicas.remove(entity.persistanceKey());
    modifiedCorruptReplicas.remove(entity.persistanceKey());
    removedCorruptReplicas.put(entity.persistanceKey(), entity);
    log("removed-corrupt", CacheHitState.NA, 
            new String[]{"bid", Long.toString(entity.getBlockId()), "sid", entity.getStorageId()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(CorruptReplica entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private List<CorruptReplica> syncCorruptReplicaInstances(List<CorruptReplica> crs) {

    ArrayList<CorruptReplica> finalList = new ArrayList<CorruptReplica>();

    for (CorruptReplica replica : crs) {
      if (removedCorruptReplicas.containsKey(replica.persistanceKey())) {
        continue;
      }
      if (corruptReplicas.containsKey(replica.persistanceKey())) {
        finalList.add(corruptReplicas.get(replica.persistanceKey()));
      } else {
        corruptReplicas.put(replica.persistanceKey(), replica);
        finalList.add(replica);
      }
    }

    return finalList;
  }
}
