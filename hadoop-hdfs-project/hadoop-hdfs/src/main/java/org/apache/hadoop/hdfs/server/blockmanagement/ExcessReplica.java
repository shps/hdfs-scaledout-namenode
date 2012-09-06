package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplica extends Replica {

  public static enum Counter implements org.apache.hadoop.hdfs.server.namenode.CounterType<ExcessReplica> {

    All;

    @Override
    public Class getType() {
      return ExcessReplica.class;
    }
  }

  public static enum Finder implements org.apache.hadoop.hdfs.server.namenode.FinderType<ExcessReplica> {

    ByStorageId, ByPKey, ByBlockId;

    @Override
    public Class getType() {
      return ExcessReplica.class;
    }
  }

  public ExcessReplica(String storageId, long blockId) {
    super(storageId, blockId);
  }
}
