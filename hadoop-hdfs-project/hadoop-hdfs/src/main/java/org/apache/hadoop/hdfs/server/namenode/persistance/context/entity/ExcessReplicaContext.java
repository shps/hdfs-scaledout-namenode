package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ExcessReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaContext extends EntityContext<ExcessReplica> {

  private Map<ExcessReplica, ExcessReplica> exReplicas = new HashMap<ExcessReplica, ExcessReplica>();
  private Map<String, TreeSet<ExcessReplica>> storageIdToExReplica = new HashMap<String, TreeSet<ExcessReplica>>();
  private Map<ExcessReplica, ExcessReplica> newExReplica = new HashMap<ExcessReplica, ExcessReplica>();
  private Map<ExcessReplica, ExcessReplica> removedExReplica = new HashMap<ExcessReplica, ExcessReplica>();
  private ExcessReplicaDataAccess dataAccess;

  public ExcessReplicaContext(ExcessReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(ExcessReplica exReplica) throws TransactionContextException {
    if (removedExReplica.containsKey(exReplica)) {
      throw new TransactionContextException("Removed excess-replica passed to be persisted");
    }

    exReplicas.put(exReplica, exReplica);
    newExReplica.put(exReplica, exReplica);
    log("added-replica", CacheHitState.NA,
            new String[]{"bid", Long.toString(exReplica.getBlockId()), "sid", exReplica.getStorageId()});
  }

  @Override
  public void clear() {
    exReplicas.clear();
    storageIdToExReplica.clear();
    newExReplica.clear();
    removedExReplica.clear();
    super.clear();
  }

  @Override
  public int count(CounterType<ExcessReplica> counter, Object... params) throws PersistanceException {
    ExcessReplica.Counter eCounter = (ExcessReplica.Counter) counter;
    switch (eCounter) {
      case All:
        log("count-all");
        return dataAccess.countAll();
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public ExcessReplica find(FinderType<ExcessReplica> finder,
          Object... params) throws PersistanceException {
    ExcessReplica.Finder eFinder = (ExcessReplica.Finder) finder;
    ExcessReplica result = null;

    switch (eFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        String storageId = (String) params[1];
        ExcessReplica searchInstance = new ExcessReplica(storageId, blockId);
        if (exReplicas.containsKey(searchInstance)) {
          log("find-by-pk", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", storageId});
          result = exReplicas.get(searchInstance);
        } else if (removedExReplica.containsKey(searchInstance)) {
          log("find-by-pk-removed-item", CacheHitState.LOSS,
                  new String[]{"bid", Long.toString(blockId), "sid", storageId});
          result = null;
        } else {
          log("find-by-pk", CacheHitState.LOSS,
                  new String[]{"bid", Long.toString(blockId), "sid", storageId});
          result = dataAccess.findByPkey(params);
          this.exReplicas.put(result, result);
        }
        return result;
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<ExcessReplica> findList(FinderType<ExcessReplica> finder, Object... params) throws PersistanceException {
    ExcessReplica.Finder eFinder = (ExcessReplica.Finder) finder;
    TreeSet<ExcessReplica> result = null;

    switch (eFinder) {
      case ByStorageId:
        String sId = (String) params[0];
        if (storageIdToExReplica.containsKey(sId)) {
          log("find-by-storageid", CacheHitState.HIT, new String[]{"sid", sId});
          result = storageIdToExReplica.get(sId);
        } else {
          log("find-by-storageid", CacheHitState.LOSS, new String[]{"sid", sId});
          result = syncExcessReplicaInstances(dataAccess.findExcessReplicaByStorageId(sId));
          storageIdToExReplica.put(sId, result);
        }
        return result;
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedExReplica.values(), newExReplica.values(), null);
    log("prepared");
  }

  @Override
  public void remove(ExcessReplica exReplica) throws PersistanceException {
    if (exReplicas.remove(exReplica) == null) {
      throw new TransactionContextException("Unattached excess-replica passed to be removed");
    }

    newExReplica.remove(exReplica);
    removedExReplica.put(exReplica, exReplica);
    log("removed", CacheHitState.NA,
            new String[]{"bid", Long.toString(exReplica.getBlockId()), "sid", exReplica.getStorageId()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(ExcessReplica entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private TreeSet<ExcessReplica> syncExcessReplicaInstances(List<ExcessReplica> list) {
    TreeSet<ExcessReplica> replicaSet = new TreeSet<ExcessReplica>();

    for (ExcessReplica replica : list) {
      if (!removedExReplica.containsKey(replica)) {
        if (exReplicas.containsKey(replica)) {
          replicaSet.add(exReplicas.get(replica));
        } else {
          exReplicas.put(replica, replica);
          replicaSet.add(replica);
        }
      }
    }

    return replicaSet;
  }
}