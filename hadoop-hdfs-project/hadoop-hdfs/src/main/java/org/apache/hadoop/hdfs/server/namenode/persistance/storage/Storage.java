package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public interface Storage<T> {
  
  public void remove(T entity) throws TransactionContextException;
  
  public Collection<T> findList(Finder<T> finder, Object ... params);
  public T find(Finder<T> finder, Object ... params);
  
  public int countAll();
  
  public void update(T entity) throws TransactionContextException;
  public void add(T entity) throws TransactionContextException;
  
  public void commit();
  
  public void clear();
}
