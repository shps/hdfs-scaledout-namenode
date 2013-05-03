package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class UnderReplicatedBlockDataAccess extends EntityDataAccess {

  public static final String TABLE_NAME = "under_replicated_blocks";
  public static final String BLOCK_ID = "block_id";
  public static final String LEVEL = "level";

  public abstract UnderReplicatedBlock findByBlockId(long blockId) throws StorageException;

  public abstract List<UnderReplicatedBlock> findAll() throws StorageException;

  public abstract List<UnderReplicatedBlock> findByLevel(int level) throws StorageException;

  public abstract List<UnderReplicatedBlock> findAllLessThanLevel(int level) throws StorageException;

  public abstract void prepare(Collection<UnderReplicatedBlock> removed, Collection<UnderReplicatedBlock> newed, Collection<UnderReplicatedBlock> modified) throws StorageException;

  public abstract void removeAll() throws StorageException;

  public abstract int countAll() throws StorageException;
  
  public abstract int countByLevel(int level) throws StorageException;
  
  public abstract int countLessThanALevel(int level) throws StorageException;

}
