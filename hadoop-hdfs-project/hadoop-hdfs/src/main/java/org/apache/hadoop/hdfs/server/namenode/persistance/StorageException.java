package org.apache.hadoop.hdfs.server.namenode.persistance;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageException extends Exception {

  public StorageException(String message) {
    super(message);
  }
  
  public StorageException(Throwable ex)
  {
    super(ex);
  }
}
