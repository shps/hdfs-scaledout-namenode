package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public interface EntityContext<T> {

  public void remove(T entity) throws TransactionContextException;
  
  public void removeAll() throws TransactionContextException;

  public Collection<T> findList(FinderType<T> finder, Object... params);

  public T find(FinderType<T> finder, Object... params);

  public int count(CounterType<T> counter, Object... params);

  public void update(T entity) throws TransactionContextException;

  public void add(T entity) throws TransactionContextException;

  public void prepare();

  public void clear();
}
