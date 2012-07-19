package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public interface InvalidateBlockDataAccess {

  public static final String TABLE_NAME = "invalidated_blocks";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";
  public static final String GENERATION_STAMP = "generation_stamp";
  public static final String NUM_BYTES = "num_bytes";

  public int countAll() throws StorageException;

  public Collection<InvalidatedBlock> findInvalidatedBlockByStorageId(String storageId) throws StorageException;

  public Collection<InvalidatedBlock> findAllInvalidatedBlocks() throws StorageException;

  public InvalidatedBlock findInvBlockByPkey(Object[] params) throws StorageException;

  public void prepare(Collection<InvalidatedBlock> removed, Collection<InvalidatedBlock> newed, Collection<InvalidatedBlock> modified) throws StorageException;
}
