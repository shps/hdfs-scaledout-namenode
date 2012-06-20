package org.apache.hadoop.hdfs.server.namenode.persistance;

/**
 *
 * @author kamal
 */
class TransactionContextException extends Exception {

  public TransactionContextException(String msg) {
    super(msg);
  }
  
  public TransactionContextException(Exception e) {
    super(e);
  }
  
}
