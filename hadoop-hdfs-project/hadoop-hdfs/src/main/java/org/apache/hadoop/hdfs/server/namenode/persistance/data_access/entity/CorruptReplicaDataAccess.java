package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public interface CorruptReplicaDataAccess {

  public static final String TABLE_NAME = "corrupt_replicas";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";

  public abstract int countAll() throws StorageException;
  
  public abstract CorruptReplica findByPk(long blockId, String storageId) throws StorageException;

  public abstract List<CorruptReplica> findAll() throws StorageException;

  public abstract List<CorruptReplica> findByBlockId(long blockId) throws StorageException;

  public abstract void prepare(Collection<CorruptReplica> removed, Collection<CorruptReplica> newed, Collection<CorruptReplica> modified) throws StorageException;
  
}
