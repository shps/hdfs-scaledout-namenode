package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.EntityContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class TransactionContext {

  private static Log logger = LogFactory.getLog(TransactionContext.class);
  private static String UNKNOWN_TYPE = "Unknown type:";
  private boolean activeTxExpected = false;
  private Map<Class, EntityContext> typeContextMap;
  private Set<EntityContext> contexts = new HashSet<EntityContext>();
  private StorageConnector connector;

  public TransactionContext(StorageConnector connector, Map<Class, EntityContext> entityContext) {
    this.typeContextMap = entityContext;
    for (EntityContext context : entityContext.values()) {
      if (!contexts.contains(context)) {
        contexts.add(context);
      }
    }
    this.connector = connector;
  }

  private void resetContext() {
    activeTxExpected = false;
    clearContext();
    EntityContext.setLockMode(null); // null won't be logged
  }

  public void clearContext() {
    for (EntityContext context : contexts) {
      context.clear();
    }
  }

  public void begin() throws StorageException {
    activeTxExpected = true;
    connector.beginTransaction();
    logger.debug("begin");
  }

  public void commit() throws StorageException {
    aboutToPerform();

    for (EntityContext context : contexts) {
      context.prepare();
    }

    resetContext();

    connector.commit();
    logger.debug("commit");
  }

  public void rollback() throws StorageException {
    resetContext();
    connector.rollback();
    logger.debug("rollback");
  }

  public <T> void update(T obj) throws PersistanceException {
    aboutToPerform();

    if (typeContextMap.containsKey(obj.getClass())) {
      typeContextMap.get(obj.getClass()).update(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public <T> void add(T obj) throws PersistanceException {
    aboutToPerform();

    if (typeContextMap.containsKey(obj.getClass())) {
      typeContextMap.get(obj.getClass()).add(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public <T> void remove(T obj) throws PersistanceException {
    aboutToPerform();

    if (typeContextMap.containsKey(obj.getClass())) {
      typeContextMap.get(obj.getClass()).remove(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public void removeAll(Class type) throws PersistanceException {
    aboutToPerform();

    if (typeContextMap.containsKey(type)) {
      typeContextMap.get(type).removeAll();
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + type);
    }
  }

  public <T> T find(FinderType<T> finder, Object... params) throws PersistanceException {
    aboutToPerform();
    if (typeContextMap.containsKey(finder.getType())) {
//      logger.debug("TX-Find: " + finder.getType().getName());
      return (T) typeContextMap.get(finder.getType()).find(finder, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + finder.getType());
    }
  }

  public <T> Collection<T> findList(FinderType<T> finder, Object... params) throws PersistanceException {
    aboutToPerform();
    if (typeContextMap.containsKey(finder.getType())) {
//      logger.debug("TX-FindList: " + finder.getType().getName());
      return typeContextMap.get(finder.getType()).findList(finder, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + finder.getType());
    }
  }

  public int count(CounterType counter, Object... params) throws PersistanceException {
    aboutToPerform();
    if (typeContextMap.containsKey(counter.getType())) {
//      logger.debug("TX-Count: " + counter.getType().getName());
      return typeContextMap.get(counter.getType()).count(counter, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + counter.getType());
    }
  }

  private void aboutToPerform() throws StorageException {
    if (activeTxExpected && !connector.isTransactionActive()) {
      throw new StorageException("Active transaction is expected while storage doesn't have it.");
    } else if (!activeTxExpected) {
      throw new RuntimeException("Transaction is not begun.");
    }
  }
}
