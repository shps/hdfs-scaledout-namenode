package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.log4j.Logger;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class EntityContext<T> {

  static String NOT_SUPPORTED_YET = "Not supported yet.";
  static String UNSUPPORTED_FINDER = "Unsupported finder.";
  static String UNSUPPORTED_COUNTER = "Unsupported counter.";
  private static final Logger LOG = Logger.getLogger(TransactionContext.class);

  /**
   * Defines the cache state of the request. This enum is only used for logging purpose.
   */
  public enum CacheHitState {

    HIT, LOSS, NA
  }

  public abstract void add(T entity) throws PersistanceException;

  public void clear()
  {
    log("cleared-context");
  }

  public abstract int count(CounterType<T> counter, Object... params) throws PersistanceException;

  public abstract T find(FinderType<T> finder, Object... params) throws PersistanceException;

  public abstract Collection<T> findList(FinderType<T> finder, Object... params) throws PersistanceException;

  public abstract void prepare() throws StorageException;

  public abstract void remove(T entity) throws PersistanceException;

  public abstract void removeAll() throws PersistanceException;

  public abstract void update(T entity) throws PersistanceException;

  public void log(String opName, CacheHitState state, String... params) {
    StringBuilder message = new StringBuilder();
    message.append(opName).append(" ");
    if (state == CacheHitState.HIT) {
      message.append("hit");
    } else if (state == CacheHitState.LOSS) {
      message.append("loss");
    }
    message.append(" ");
    if (params != null) {
      for (int i = 0; i < params.length; i = i + 2) {
        message.append(" ").append(params[i]);
        message.append("=").append(params[i + 1]);
      }
    }
    LOG.debug(message.toString());
  }

  public void log(String opName) {
    log(opName, CacheHitState.NA, (String) null);
  }

  public void log(String opName, CacheHitState state) {
    log(opName, state, (String) null);
  }
}
