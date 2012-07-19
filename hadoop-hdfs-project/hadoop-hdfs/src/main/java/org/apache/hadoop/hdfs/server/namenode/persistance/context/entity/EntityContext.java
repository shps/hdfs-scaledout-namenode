package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public interface EntityContext<T> {

  static String NOT_SUPPORTED_YET = "Not supported yet.";
  static String UNSUPPORTED_FINDER = "Unsupported finder.";
  static String UNSUPPORTED_COUNTER = "Unsupported counter.";
  
  public void add(T entity) throws PersistanceException;
  
  public void clear();
  
  public int count(CounterType<T> counter, Object... params) throws PersistanceException;
  
  public T find(FinderType<T> finder, Object... params) throws PersistanceException;
  
  public Collection<T> findList(FinderType<T> finder, Object... params) throws PersistanceException;
  
  public void prepare() throws StorageException;
  
  public void remove(T entity) throws PersistanceException;
  
  public void removeAll() throws PersistanceException;

  public void update(T entity) throws PersistanceException;
}
