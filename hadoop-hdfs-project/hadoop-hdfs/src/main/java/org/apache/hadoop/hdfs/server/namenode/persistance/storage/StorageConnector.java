
package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public interface StorageConnector<T> {
  
  public void setConfiguration(Configuration conf);
  
  public <T> T obtainSession();
  
  public void beginTransaction();
  
  public void commit() throws StorageException;
  
  public void rollback();
  
  public boolean formatStorage();
  
  public boolean isTransactionActive();
  
  public void stopStorage();
}
