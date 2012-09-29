package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 *
 * @author jude
 */
public class CorruptReplica extends Replica {
  public static enum Counter implements org.apache.hadoop.hdfs.server.namenode.CounterType<CorruptReplica> {

    All;

    @Override
    public Class getType() {
      return CorruptReplica.class;
    }
    
  }
  
  public static enum Finder implements org.apache.hadoop.hdfs.server.namenode.FinderType<CorruptReplica> {

    All, ByBlockId, ByPk;

    @Override
    public Class getType() {
      return CorruptReplica.class;
    }
    
  }

  public CorruptReplica(long blockId, String storageId) {
    super(storageId, blockId);
  }

//  public String persistanceKey() {
//    return blockId + storageId;
//  }
}
