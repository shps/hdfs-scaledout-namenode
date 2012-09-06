package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class ExcessReplicaDataAccess extends EntityDataAccess {

  public static final String TABLE_NAME = "excess_replicas";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";

  public abstract int countAll() throws StorageException;

  public abstract List<ExcessReplica> findExcessReplicaByStorageId(String sId) throws StorageException;
  public abstract List<ExcessReplica> findExcessReplicaByBlockId(long bId) throws StorageException;

  public abstract ExcessReplica findByPkey(Object[] params) throws StorageException;
  
  public abstract void prepare(Collection<ExcessReplica> removed, Collection<ExcessReplica> newed, Collection<ExcessReplica> modified) throws StorageException;
}
