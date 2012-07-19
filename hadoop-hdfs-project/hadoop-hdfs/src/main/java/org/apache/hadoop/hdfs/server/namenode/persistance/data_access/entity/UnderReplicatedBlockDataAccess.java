package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public interface UnderReplicatedBlockDataAccess {

  public static final String TABLE_NAME = "under_replicated_blocks";
  public static final String BLOCK_ID = "block_id";
  public static final String LEVEL = "level";

  public UnderReplicatedBlock findByBlockId(long blockId) throws StorageException;

  public List<UnderReplicatedBlock> findAllSortedByLevel() throws StorageException;

  public List<UnderReplicatedBlock> findByLevel(int level) throws StorageException;

  public List<UnderReplicatedBlock> findAllLessThanLevel(int level) throws StorageException;

  public void prepare(Collection<UnderReplicatedBlock> removed, Collection<UnderReplicatedBlock> newed, Collection<UnderReplicatedBlock> modified) throws StorageException;

  public void removeAll() throws StorageException;
}
