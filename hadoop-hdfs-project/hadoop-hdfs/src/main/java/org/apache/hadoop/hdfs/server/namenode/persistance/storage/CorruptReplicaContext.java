package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.Counter;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public abstract class CorruptReplicaContext implements EntityContext<CorruptReplica> {

  protected Map<String, CorruptReplica> corruptReplicas = new HashMap<String, CorruptReplica>();
  protected Map<Long, List<CorruptReplica>> blockCorruptReplicas = new HashMap<Long, List<CorruptReplica>>();
  protected Map<String, CorruptReplica> modifiedCorruptReplicas = new HashMap<String, CorruptReplica>();
  protected Map<String, CorruptReplica> removedCorruptReplicas = new HashMap<String, CorruptReplica>();
  protected boolean allCorruptBlocksRead = false;

  @Override
  public void remove(CorruptReplica entity) throws TransactionContextException {
    if (corruptReplicas.get(entity.persistanceKey()) == null) {
      throw new TransactionContextException("Unattached corrupt replica passed to be removed");
    }

    corruptReplicas.remove(entity.persistanceKey());
    modifiedCorruptReplicas.remove(entity.persistanceKey());
    removedCorruptReplicas.put(entity.persistanceKey(), entity);

  }

  @Override
  public Collection<CorruptReplica> findList(Finder<CorruptReplica> finder, Object... params) {
    CorruptReplica.Finder cFinder = (CorruptReplica.Finder) finder;

    switch (cFinder) {
      case All:
        if (allCorruptBlocksRead) {
          return corruptReplicas.values();
        }
        List<CorruptReplica> sync = syncCorruptReplicaInstances(findAll());
        allCorruptBlocksRead = true;
        return sync;
      case ByBlockId:
        Long blockId = (Long) params[0];
        if (blockCorruptReplicas.containsKey(blockId)) {
          return blockCorruptReplicas.get(blockId);
        }

        List<CorruptReplica> syncList = syncCorruptReplicaInstances(findByBlockId(blockId));
        blockCorruptReplicas.put(blockId, syncList);
        return syncList;

    }

    return null;
  }

  @Override
  public CorruptReplica find(Finder<CorruptReplica> finder, Object... params) {
    CorruptReplica.Finder cFinder = (CorruptReplica.Finder) finder;

    switch (cFinder) {
      case ByPk:
        Long blockId = (Long) params[0];
        String storageId = (String) params[1];
        if (corruptReplicas.containsKey(blockId + storageId)) {
          return corruptReplicas.get(blockId + storageId);
        } else {
          return findByPk(blockId, storageId);
        }
    }
    return null;
  }

  @Override
  public int count(Counter<CorruptReplica> counter, Object... params) {
    CorruptReplica.Counter cCounter = (CorruptReplica.Counter) counter;

    switch (cCounter) {
      case All:
        if (allCorruptBlocksRead) {
          return corruptReplicas.size();
        }
        return findAll().size();
    }
    
    return -1;
  }

  @Override
  public void update(CorruptReplica entity) throws TransactionContextException {
    if (removedCorruptReplicas.get(entity.persistanceKey()) != null) {
      throw new TransactionContextException("Removed corrupt replica passed to be persisted");
    }
    corruptReplicas.put(entity.persistanceKey(), entity);
    modifiedCorruptReplicas.put(entity.persistanceKey(), entity);
  }

  @Override
  public void add(CorruptReplica entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void clear() {
    corruptReplicas.clear();
    blockCorruptReplicas.clear();
    modifiedCorruptReplicas.clear();
    removedCorruptReplicas.clear();
    allCorruptBlocksRead = false;
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

  protected abstract CorruptReplica findByPk(long blockId, String storageId);

  protected abstract List<CorruptReplica> findAll();

  protected abstract List<CorruptReplica> findByBlockId(long blockId);
}
