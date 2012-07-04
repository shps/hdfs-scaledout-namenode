package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.EntityContext;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class TransactionContext {

  private static Log logger = LogFactory.getLog(TransactionContext.class);
  private boolean activeTxExpected = false;
  private boolean externallyMngedTx = true;
  private Map<Class, EntityContext> entityContexts;
  private StorageConnector connector;

  public TransactionContext(StorageConnector connector, Map<Class, EntityContext> storages) {
    this.entityContexts = storages;
    this.connector = connector;
  }

  private void resetContext() {
    activeTxExpected = false;
    externallyMngedTx = true;

    for (EntityContext context : entityContexts.values()) {
      context.clear();
    }
  }

  public void begin() {
    activeTxExpected = true;
    connector.beginTransaction();
    logger.debug("\nTX begin{");
  }

  public void commit() throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Active transaction is expected.");
    }

    for (EntityContext context : entityContexts.values()) {
      context.prepare();
      context.clear();
    }

    resetContext();
    activeTxExpected = false;
    externallyMngedTx = true;
    try {
      connector.commit();
    } catch (StorageException ex) {
      throw new TransactionContextException(ex);
    }
  }

  public void rollback() {
    resetContext();
    connector.rollback();
    logger.debug("\n}Tx rollback");
  }

  public <T> void update(T obj) throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Transaction was not begun");
    }

    if (entityContexts.containsKey(obj.getClass())) {
      entityContexts.get(obj.getClass()).update(obj);
    } else {
      throw new TransactionContextException("Unknown class class type to add. Class type: " + obj.getClass());
    }
  }

  public <T> void add(T obj) throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Transaction was not begun");
    }
    if (entityContexts.containsKey(obj.getClass())) {
      entityContexts.get(obj.getClass()).add(obj);
    } else {
      throw new TransactionContextException("Unknown class class type to add. Class type: " + obj.getClass());
    }
  }

  public <T> void remove(T obj) throws TransactionContextException {
    beforeTxCheck();
    boolean done = true;
    try {
      if (entityContexts.containsKey(obj.getClass())) {
        entityContexts.get(obj.getClass()).remove(obj);
      } else {
        done = false;
        throw new TransactionContextException("Unkown type passed for being persisted");
      }
    } finally {
      afterTxCheck(done);
    }
  }

  public void removeAll(Class type) throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Transaction was not begun");
    }
    if (entityContexts.containsKey(type)) {
      entityContexts.get(type).removeAll();
    } else {
      throw new TransactionContextException("Unknown class passed to remove all: " + type);
    }
  }

  public <T> T find(FinderType<T> finder, Object... params) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (entityContexts.containsKey(finder.getType())) {
        return (T) entityContexts.get(finder.getType()).find(finder, params);
      } else {
        throw new TransactionContextException("Unknown class class type to add. Class type: " + finder.getType());
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public <T> Collection<T> findList(FinderType<T> finder, Object... params) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (entityContexts.containsKey(finder.getType())) {
        return entityContexts.get(finder.getType()).findList(finder, params);
      } else {
        throw new TransactionContextException("Unknown class class type to add. Class type: " + finder.getType());
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public int count(CounterType counter, Object... params) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (entityContexts.containsKey(counter.getType())) {
        return entityContexts.get(counter.getType()).count(counter, params);
      } else {
        throw new TransactionContextException("Unknown class class type to add. Class type: " + counter.getType());
      }
    } finally {
      afterTxCheck(true);
    }
  }

  private void beforeTxCheck() throws TransactionContextException {
    if (activeTxExpected && !connector.isTransactionActive()) {
      throw new TransactionContextException("Active transaction is expected.");
    } else if (!activeTxExpected) {
      connector.beginTransaction();
      externallyMngedTx = false;
    }
  }

  private void afterTxCheck(boolean done) {
    if (!externallyMngedTx) {
      if (done) {
        try {
          connector.commit();
        } catch (StorageException ex) {
          Logger.getLogger(TransactionContext.class.getName()).log(Level.SEVERE, null, ex);
        }
      } else {
        connector.rollback();
      }
    }
  }
}
