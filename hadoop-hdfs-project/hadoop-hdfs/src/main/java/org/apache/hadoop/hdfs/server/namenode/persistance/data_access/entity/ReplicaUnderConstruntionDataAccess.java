package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh
 */
public abstract class ReplicaUnderConstruntionDataAccess extends EntityDataAccess {

  public static final String TABLE_NAME = "replica_under_constructions";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";
  public static final String STATE = "state";
  public static final String REPLICA_INDEX = "replica_index";

  public abstract List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId) throws StorageException;

  public abstract void prepare(Collection<ReplicaUnderConstruction> removed, Collection<ReplicaUnderConstruction> newed, Collection<ReplicaUnderConstruction> modified) throws StorageException;
}
