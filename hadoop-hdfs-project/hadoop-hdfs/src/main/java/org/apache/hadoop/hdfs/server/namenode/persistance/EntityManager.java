package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.BlockInfoClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.Storage;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ExcessReplicaClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.IndexedReplicaClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.InvalidatedBlockClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.LeaseClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.LeasePathClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.PendingBlockClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ReplicaUnderConstructionClusterj;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class EntityManager {

  private static EntityManager instance;

  private EntityManager() {
  }

  public synchronized static EntityManager getInstance() {
    if (instance == null) {
      instance = new EntityManager();
    }

    return instance;
  }
  ThreadLocal<TransactionContext> contexts = new ThreadLocal<TransactionContext>();

  private TransactionContext context() {
    TransactionContext context = contexts.get();

    if (context == null) {
      context = new TransactionContext(buildStorages());
      contexts.set(context);
    }
    return context;
  }

  private Map<Class, Storage> buildStorages() {
    Map<Class, Storage> storages = new HashMap<Class, Storage>();
    BlockInfoClusterj bicj = new BlockInfoClusterj();
    storages.put(BlockInfo.class, bicj);
    storages.put(BlockInfoUnderConstruction.class, bicj);
    storages.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionClusterj());
    storages.put(IndexedReplica.class, new IndexedReplicaClusterj());
    storages.put(ExcessReplica.class, new ExcessReplicaClusterj());
    storages.put(InvalidatedBlock.class, new InvalidatedBlockClusterj());
    storages.put(Lease.class, new LeaseClusterj());
    storages.put(LeasePath.class, new LeasePathClusterj());
    storages.put(PendingBlockInfo.class, new PendingBlockClusterj());
    
    return storages;
  }

  public void begin() {
    context().begin();
  }

  public void commit() {
    try {
      context().commit();
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void rollback() {
    context().rollback();
  }

  public <T> void remove(T obj) {
    try {
      context().remove(obj);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public <T> Collection<T> findList(Finder<T> finder, Object... params) {
    try {
      return context().findList(finder, params);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public <T> T find(Finder<T> finder, Object... params) {
    try {
      return context().find(finder, params);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public int countAll(Class classType) {
    try {
      return context().countAll(classType);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return -1;
  }

  public <T> void update(T entity) {
    try {
      context().update(entity);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public <T> void add(T entity) {
    try {
      context().add(entity);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
