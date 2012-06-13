package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
@PersistenceCapable(table = "PendingReplicationBlock")
public interface PendingReplicationBlockTable {

  @PrimaryKey
  @Column(name = "blockId")
  long getBlockId();

  void setBlockId(long blockId);

  @Column(name = "timestamp")
  long getTimestamp();

  void setTimestamp(long timestamp);

  @Column(name = "numReplicasInProgress")
  int getNumReplicasInProgress();

  void setNumReplicasInProgress(int numReplicasInProgress);
}
