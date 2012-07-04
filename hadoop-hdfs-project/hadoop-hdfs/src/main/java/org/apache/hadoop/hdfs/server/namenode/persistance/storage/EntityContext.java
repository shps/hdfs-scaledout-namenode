package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.persistance.Counter;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public interface EntityContext<T> {

  public void remove(T entity) throws TransactionContextException;
  
  public void removeAll() throws TransactionContextException;

  public Collection<T> findList(Finder<T> finder, Object... params);

  public T find(Finder<T> finder, Object... params);

  public int count(Counter<T> counter, Object... params);

  public void update(T entity) throws TransactionContextException;

  public void add(T entity) throws TransactionContextException;

  public void prepare();

  public void clear();
}
