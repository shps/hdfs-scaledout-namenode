package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class ExcessReplicaStorage implements Storage<ExcessReplica> {

  public static final String TABLE_NAME = "excess_replicas";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";
  protected Map<ExcessReplica, ExcessReplica> exReplicas = new HashMap<ExcessReplica, ExcessReplica>();
  protected Map<String, TreeSet<ExcessReplica>> storageIdToExReplica = new HashMap<String, TreeSet<ExcessReplica>>();
  protected Map<ExcessReplica, ExcessReplica> newExReplica = new HashMap<ExcessReplica, ExcessReplica>();
  protected Map<ExcessReplica, ExcessReplica> removedExReplica = new HashMap<ExcessReplica, ExcessReplica>();

  @Override
  public void clear() {
    exReplicas.clear();
    storageIdToExReplica.clear();
    newExReplica.clear();
    removedExReplica.clear();
  }

  @Override
  public void remove(ExcessReplica exReplica) throws TransactionContextException {
    if (exReplicas.remove(exReplica) == null) {
      throw new TransactionContextException("Unattached excess-replica passed to be removed");
    }

    newExReplica.remove(exReplica);
    removedExReplica.put(exReplica, exReplica);
  }

  @Override
  public Collection<ExcessReplica> findList(Finder<ExcessReplica> finder, Object... params) {
    ExcessReplicaFinder eFinder = (ExcessReplicaFinder) finder;
    TreeSet<ExcessReplica> result = null;

    switch (eFinder) {
      case ByStorageId:
        String sId = (String) params[0];
        if (storageIdToExReplica.containsKey(sId)) {
          result = storageIdToExReplica.get(sId);
        } else {
          result = findExcessReplicaByStorageId(sId);
          storageIdToExReplica.put(sId, result);
        }
        break;
    }
    return result;
  }

  @Override
  public ExcessReplica find(Finder<ExcessReplica> finder,
          Object... params) {
    ExcessReplicaFinder eFinder = (ExcessReplicaFinder) finder;
    ExcessReplica result = null;

    switch (eFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        String storageId = (String) params[1];
        ExcessReplica searchInstance = new ExcessReplica(storageId, blockId);
        if (exReplicas.containsKey(searchInstance)) {
          result = exReplicas.get(searchInstance);
        } else if (removedExReplica.containsKey(searchInstance)) {
          result = null;
        } else {
          result = findByPkey(params);
          this.exReplicas.put(result, result);
        }
        break;
    }

    return result;
  }

  @Override
  public void add(ExcessReplica exReplica) throws TransactionContextException {
    if (removedExReplica.containsKey(exReplica)) {
      throw new TransactionContextException("Removed excess-replica passed to be persisted");
    }

    exReplicas.put(exReplica, exReplica);
    newExReplica.put(exReplica, exReplica);
  }

  @Override
  public void update(ExcessReplica entity) throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  protected abstract TreeSet<ExcessReplica> findExcessReplicaByStorageId(String sId);

  protected abstract ExcessReplica findByPkey(Object[] params);
}