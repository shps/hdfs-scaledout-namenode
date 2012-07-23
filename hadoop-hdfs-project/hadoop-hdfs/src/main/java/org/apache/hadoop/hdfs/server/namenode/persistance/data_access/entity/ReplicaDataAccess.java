package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public abstract class ReplicaDataAccess extends EntityDataAccess {

  public static final String TABLE_NAME = "replicas";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";
  public static final String REPLICA_INDEX = "replica_index";

  public abstract List<IndexedReplica> findReplicasById(long id) throws StorageException;

  public abstract void prepare(Collection<IndexedReplica> removed, Collection<IndexedReplica> newed, Collection<IndexedReplica> modified) throws StorageException;
}