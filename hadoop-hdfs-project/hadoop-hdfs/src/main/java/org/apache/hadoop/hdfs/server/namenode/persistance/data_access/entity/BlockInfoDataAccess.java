package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public abstract class BlockInfoDataAccess extends EntityDataAccess {

  /**
   * block info table info
   */
  public static final String TABLE_NAME = "block_infos";
  public static final String BLOCK_ID = "block_id";
  public static final String BLOCK_INDEX = "block_index";
  public static final String INODE_ID = "inode_id";
  public static final String NUM_BYTES = "num_bytes";
  public static final String GENERATION_STAMP = "generation_stamp";
  public static final String BLOCK_UNDER_CONSTRUCTION_STATE = "block_under_construction_state";
  public static final String TIME_STAMP = "time_stamp";
  public static final String PRIMARY_NODE_INDEX = "primary_node_index";
  public static final String BLOCK_RECOVERY_ID = "block_recovery_id";

  public abstract int countAll() throws StorageException;
  
  public abstract BlockInfo findById(long blockId) throws StorageException;

  public abstract List<BlockInfo> findByInodeId(long id) throws StorageException;

  public abstract List<BlockInfo> findAllBlocks() throws StorageException;

  public abstract List<BlockInfo> findByStorageId(String storageId) throws StorageException;
  
  public abstract void prepare(Collection<BlockInfo> removed, Collection<BlockInfo> newed, Collection<BlockInfo> modified) throws StorageException;

}
