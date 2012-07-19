package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public interface ExcessReplicaDataAccess {

  public static final String TABLE_NAME = "excess_replicas";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";

  public int countAll() throws StorageException;

  public List<ExcessReplica> findExcessReplicaByStorageId(String sId) throws StorageException;

  public ExcessReplica findByPkey(Object[] params) throws StorageException;
  
  public void prepare(Collection<ExcessReplica> removed, Collection<ExcessReplica> newed, Collection<ExcessReplica> modified) throws StorageException;
}
