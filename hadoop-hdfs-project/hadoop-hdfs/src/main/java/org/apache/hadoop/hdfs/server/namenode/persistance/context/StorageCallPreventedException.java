package org.apache.hadoop.hdfs.server.namenode.persistance.context;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public class StorageCallPreventedException extends TransactionContextException {
    public StorageCallPreventedException(String msg) {
    super(msg);
  }
}
