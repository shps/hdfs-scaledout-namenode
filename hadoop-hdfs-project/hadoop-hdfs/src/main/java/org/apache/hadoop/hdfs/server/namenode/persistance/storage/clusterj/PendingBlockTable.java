package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.PendingBlockStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
@PersistenceCapable(table = PendingBlockStorage.TABLE_NAME)
public interface PendingBlockTable {

  @PrimaryKey
  @Column(name = PendingBlockStorage.BLOCK_ID)
  long getBlockId();

  void setBlockId(long blockId);

  @Column(name = PendingBlockStorage.TIME_STAMP)
  long getTimestamp();

  void setTimestamp(long timestamp);

  @Column(name = PendingBlockStorage.NUM_REPLICAS_IN_PROGRESS)
  int getNumReplicasInProgress();

  void setNumReplicasInProgress(int numReplicasInProgress);
}
