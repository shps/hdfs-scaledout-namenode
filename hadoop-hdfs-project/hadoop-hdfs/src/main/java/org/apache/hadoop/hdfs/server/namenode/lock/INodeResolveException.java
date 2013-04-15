package org.apache.hadoop.hdfs.server.namenode.lock;

import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author hooman
 */
public class INodeResolveException extends PersistanceException {

  public INodeResolveException(String message) {
    super(message);
  }

  public INodeResolveException(Throwable ex) {
    super(ex);
  }
}
