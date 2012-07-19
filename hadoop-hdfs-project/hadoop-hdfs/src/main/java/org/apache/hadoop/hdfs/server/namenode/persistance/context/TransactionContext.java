package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import java.util.Collection;
import java.util.Map;
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
  public static final int RETRY_COUNT = 3;
  private boolean activeTxExpected = false;
  private Map<Class, EntityContext> entityContexts;
  private StorageConnector connector;

  public TransactionContext(StorageConnector connector, Map<Class, EntityContext> storages) {
    this.entityContexts = storages;
    this.connector = connector;
  }

  private void resetContext() {
    activeTxExpected = false;

    for (EntityContext context : entityContexts.values()) {
      context.clear();
    }
  }
  private Boolean success = null;
  private boolean retry = true;
  private int tryCount = 0;

  public void aboutToStart() {
    success = null;
    retry = true;
    tryCount = 0;
  }

  public boolean shouldRetry() {
    return tryCount <= RETRY_COUNT && retry == Boolean.TRUE;
  }

  public void setNotSuccessfull() {
    success = false;
    retry = false;
    tryCount++;
  }

  public void setShouldRetry() {
    success = false;
    retry = true;
    tryCount++;
  }

  public boolean wasNotSuccessfull() {
    return success == Boolean.FALSE;
  }

  public void begin() {
    activeTxExpected = true;
    connector.beginTransaction();
    logger.debug("\nTX begin{");
  }

  public void commit() throws StorageException {
    aboutToPerform();

    for (EntityContext context : entityContexts.values()) {
      context.prepare();
    }

    resetContext();

    connector.commit();
  }

  public void rollback() {
    resetContext();
    connector.rollback();
    logger.debug("\n}Tx rollback");
  }

  public <T> void update(T obj) throws PersistanceException {
    aboutToPerform();

    if (entityContexts.containsKey(obj.getClass())) {
      entityContexts.get(obj.getClass()).update(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public <T> void add(T obj) throws PersistanceException {
    aboutToPerform();

    if (entityContexts.containsKey(obj.getClass())) {
      entityContexts.get(obj.getClass()).add(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public <T> void remove(T obj) throws PersistanceException {
    aboutToPerform();

    if (entityContexts.containsKey(obj.getClass())) {
      entityContexts.get(obj.getClass()).remove(obj);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + obj.getClass());
    }
  }

  public void removeAll(Class type) throws PersistanceException {
    aboutToPerform();

    if (entityContexts.containsKey(type)) {
      entityContexts.get(type).removeAll();
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + type);
    }
  }

  public <T> T find(FinderType<T> finder, Object... params) throws PersistanceException {
    aboutToPerform();
    if (entityContexts.containsKey(finder.getType())) {
      return (T) entityContexts.get(finder.getType()).find(finder, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + finder.getType());
    }
  }

  public <T> Collection<T> findList(FinderType<T> finder, Object... params) throws PersistanceException {
    aboutToPerform();
    if (entityContexts.containsKey(finder.getType())) {
      return entityContexts.get(finder.getType()).findList(finder, params);
    } else {
      throw new RuntimeException(UNKNOWN_TYPE + finder.getType());
    }
  }

  public int count(CounterType counter, Object... params) throws PersistanceException {
    aboutToPerform();
    if (entityContexts.containsKey(counter.getType())) {
      return entityContexts.get(counter.getType()).count(counter, params);
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
