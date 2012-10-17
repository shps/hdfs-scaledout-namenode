package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.EntityContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class EntityManager {

  private EntityManager() {
  }
  private static ThreadLocal<TransactionContext> contexts = new ThreadLocal<TransactionContext>();
  private static StorageConnector connector = StorageFactory.getConnector();

  private static TransactionContext context() {
    TransactionContext context = contexts.get();

    if (context == null) {
      Map<Class, EntityContext> storageMap = StorageFactory.createEntityContexts();
      context = new TransactionContext(connector, storageMap);
      contexts.set(context);
    }
    return context;
  }

  public static void begin() throws StorageException {
    context().begin();
  }

  public static void preventStorageCall() {
    context().preventStorageCall();
  }

  public static void commit() throws StorageException {
    context().commit();
  }

  public static void rollback() throws StorageException {
    context().rollback();
  }

  public static <T> void remove(T obj) throws PersistanceException {
    context().remove(obj);
  }

  public static void removeAll(Class type) throws PersistanceException {
    context().removeAll(type);
  }

  public static <T> Collection<T> findList(FinderType<T> finder, Object... params) throws PersistanceException {
    return context().findList(finder, params);
  }

  public static <T> T find(FinderType<T> finder, Object... params) throws PersistanceException {
    return context().find(finder, params);
  }

  public static int count(CounterType counter, Object... params) throws PersistanceException {
    return context().count(counter, params);
  }

  public static <T> void update(T entity) throws PersistanceException {
    context().update(entity);
  }

  public static <T> void add(T entity) throws PersistanceException {
    context().add(entity);
  }

  public static void writeLock() {
    EntityContext.setLockMode(EntityContext.LockMode.WRITE_LOCK);
    connector.writeLock();
  }

  public static void readLock() {
    EntityContext.setLockMode(EntityContext.LockMode.READ_LOCK);
    connector.readLock();
  }

  public static void readCommited() {
    EntityContext.setLockMode(EntityContext.LockMode.READ_COMMITTED);
    connector.readCommitted();
  }
  
  public static void setPartitionKey(Class name, Object key) {
    connector.setPartitionKey(name, key);
  }

  /**
   * Clears transaction context's in-memory data
   */
  public static void clearContext() {
    context().clearContext();
  }
}
