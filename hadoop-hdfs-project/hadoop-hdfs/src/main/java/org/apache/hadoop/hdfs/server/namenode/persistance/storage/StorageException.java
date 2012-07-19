package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageException extends PersistanceException {

  public StorageException(String message) {
    super(message);
  }
  
  public StorageException(Throwable ex)
  {
    super(ex);
  }
}
