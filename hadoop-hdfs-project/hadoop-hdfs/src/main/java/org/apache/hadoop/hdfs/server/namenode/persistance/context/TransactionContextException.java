package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class TransactionContextException extends PersistanceException {

  public TransactionContextException(String msg) {
    super(msg);
  }
  
}
