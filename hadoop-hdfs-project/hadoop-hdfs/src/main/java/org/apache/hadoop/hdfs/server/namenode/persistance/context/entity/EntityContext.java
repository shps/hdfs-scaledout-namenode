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
  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";

  /**
   * Defines the cache state of the request. This enum is only used for logging purpose.
   */
  public enum CacheHitState {

    HIT, LOSS, NA
  }

  public abstract void add(T entity) throws PersistanceException;

  public abstract void clear();

  public abstract int count(CounterType<T> counter, Object... params) throws PersistanceException;

  public abstract T find(FinderType<T> finder, Object... params) throws PersistanceException;

  public abstract Collection<T> findList(FinderType<T> finder, Object... params) throws PersistanceException;

  public abstract void prepare() throws StorageException;

  public abstract void remove(T entity) throws PersistanceException;

  public abstract void removeAll() throws PersistanceException;

  public abstract void update(T entity) throws PersistanceException;

  public void log(String opName, CacheHitState state, String... params) {
    StringBuilder message = new StringBuilder();
    if (state == CacheHitState.HIT) {
      message.append(ANSI_GREEN).append(opName).append(" ").append("hit").append(ANSI_RESET);
    } else if (state == CacheHitState.LOSS) {
      message.append(ANSI_RED).append(opName).append(" ").append("loss").append(ANSI_RESET);
    } else {
      message.append(opName).append(" ");
    }
    message.append(" ");
    if (params.length > 1) {
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
