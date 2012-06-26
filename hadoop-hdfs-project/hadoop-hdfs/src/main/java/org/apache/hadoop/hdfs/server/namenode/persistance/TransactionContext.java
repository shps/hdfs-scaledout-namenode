package org.apache.hadoop.hdfs.server.namenode.persistance;

import org.apache.hadoop.hdfs.server.namenode.persistance.storage.Storage;
import com.mysql.clusterj.Session;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class TransactionContext {

  private static Log logger = LogFactory.getLog(TransactionContext.class);
  private boolean activeTxExpected = false;
  private boolean externallyMngedTx = true;
  private Map<Class, Storage> storages;

  public TransactionContext(Map<Class, Storage> storages) {
    this.storages = storages;
  }

  private void resetContext() {
    activeTxExpected = false;
    externallyMngedTx = true;

    for (Storage storage : storages.values()) {
      storage.clear();
    }
  }

  void begin() {
    activeTxExpected = true;
    logger.debug("\nTX begin{");
  }

  public void commit() throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Active transaction is expected.");
    }

    for (Storage storage : storages.values()) {
      storage.commit();
      storage.clear();
    }

    activeTxExpected = false;
    externallyMngedTx = true;
  }

  void rollback() {
    resetContext();

    logger.debug("\n}Tx rollback");
  }

  public <T> void update(T obj) throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Transaction was not begun");
    }

    if (storages.containsKey(obj.getClass())) {
      storages.get(obj.getClass()).update(obj);
    } else {
      throw new TransactionContextException("Unknown class class type to add. Class type: " + obj.getClass());
    }
  }

  public <T> void add(T obj) throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Transaction was not begun");
    }
    if (storages.containsKey(obj.getClass())) {
      storages.get(obj.getClass()).add(obj);
    } else {
      throw new TransactionContextException("Unknown class class type to add. Class type: " + obj.getClass());
    }
  }

  public <T> void remove(T obj) throws TransactionContextException {
    beforeTxCheck();
    boolean done = true;
    try {
      if (storages.containsKey(obj.getClass())) {
        storages.get(obj.getClass()).remove(obj);
      } else {
        throw new TransactionContextException("Unknown class class type to add. Class type: " + obj.getClass());
      }
    } finally {
      afterTxCheck(done);
    }
  }

  public <T> T find(Finder<T> finder, Object... params) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (storages.containsKey(finder.getType())) {
        return (T) storages.get(finder.getType()).find(finder, params);
      } else {
        throw new TransactionContextException("Unknown class class type to add. Class type: " + finder.getType());
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public <T> Collection<T> findList(Finder<T> finder, Object... params) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (storages.containsKey(finder.getType())) {
        return storages.get(finder.getType()).findList(finder, params);
      } else {
        throw new TransactionContextException("Unknown class class type to add. Class type: " + finder.getType());
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public int countAll(Class type) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (storages.containsKey(type)) {
        return storages.get(type).countAll();
      } else {
        throw new TransactionContextException("Unknown class class type to add. Class type: " + type);
      }
    } finally {
      afterTxCheck(true);
    }
  }

  private void beforeTxCheck() throws TransactionContextException {
    Session session = DBConnector.obtainSession();
    if (activeTxExpected && !session.currentTransaction().isActive()) {
      throw new TransactionContextException("Active transaction is expected.");
    } else if (!activeTxExpected) {
      DBConnector.beginTransaction();
      externallyMngedTx = false;
    }
  }

  private void afterTxCheck(boolean done) {
    if (!externallyMngedTx) {
      if (done) {
        DBConnector.commit();
      } else {
        DBConnector.safeRollback();
      }
    }
  }
}
