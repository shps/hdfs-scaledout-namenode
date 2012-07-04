package org.apache.hadoop.hdfs.server.namenode.persistance;

import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.EntityContext;
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
  public static final int RETRY_COUNT = 3;

  private EntityManager() {
  }
  private static ThreadLocal<TransactionContext> contexts = new ThreadLocal<TransactionContext>();
  private static StorageConnector connector = StorageFactory.getConnector();

  private static TransactionContext context() {
    TransactionContext context = contexts.get();

    if (context == null) {
      Map<Class, EntityContext> storageMap = StorageFactory.getEntityContexts();
      context = new TransactionContext(connector, storageMap);
      contexts.set(context);
    }
    return context;
  }

  public static void begin() {
    context().begin();
  }

  public static void commit() throws TransactionContextException {
    context().commit();
  }

  public static void rollback() {
    context().rollback();
  }

  public static <T> void remove(T obj) {
    try {
      context().remove(obj);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public static void removeAll(Class type) {
    try {
      context().removeAll(type);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public static <T> Collection<T> findList(FinderType<T> finder, Object... params) {
    try {
      return context().findList(finder, params);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public static <T> T find(FinderType<T> finder, Object... params) {
    try {
      return context().find(finder, params);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public static int count(CounterType counter, Object... params) {
    try {
      return context().count(counter, params);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return -1;
  }

  public static <T> void update(T entity) {
    try {
      context().update(entity);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public static <T> void add(T entity) {
    try {
      context().add(entity);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
