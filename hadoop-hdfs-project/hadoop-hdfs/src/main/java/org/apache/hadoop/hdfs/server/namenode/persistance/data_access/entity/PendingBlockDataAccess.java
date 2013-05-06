package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public abstract class PendingBlockDataAccess extends EntityDataAccess {

  public static final String TABLE_NAME = "pending_blocks";
  public static final String BLOCK_ID = "block_id";
  public static final String TIME_STAMP = "time_stamp";
  public static final String NUM_REPLICAS_IN_PROGRESS = "num_replicas_in_progress";

  public abstract List<PendingBlockInfo> findByTimeLimit(long timeLimit) throws StorageException;

  public abstract List<PendingBlockInfo> findAll() throws StorageException;

  public abstract PendingBlockInfo findByPKey(long blockId) throws StorageException;
  
  /**
   * Counts number of pending blocks which are not expired.
   * @param timeLimit The date in long epoch. All older than the timeLimit pendingBlock are considered as expired.
   * @return number of valid pending blocks.
   * @throws StorageException 
   */
  public abstract int countValidPendingBlocks(long timeLimit) throws StorageException;

  public abstract void prepare(Collection<PendingBlockInfo> removed, Collection<PendingBlockInfo> newed, Collection<PendingBlockInfo> modified) throws StorageException;
}
