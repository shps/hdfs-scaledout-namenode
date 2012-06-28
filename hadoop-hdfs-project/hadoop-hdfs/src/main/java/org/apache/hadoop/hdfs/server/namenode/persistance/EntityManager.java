package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.Storage;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

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
  private StorageConnector connector = StorageFactory.getConnector();

  private TransactionContext context() {
    TransactionContext context = contexts.get();

    if (context == null) {
      Map<Class, Storage> storageMap = StorageFactory.getStorageMap();
      context = new TransactionContext(connector, storageMap);
      contexts.set(context);
    }
    return context;
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
