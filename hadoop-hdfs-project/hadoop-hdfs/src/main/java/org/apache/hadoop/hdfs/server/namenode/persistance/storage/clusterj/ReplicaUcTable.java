/**
 *
 */
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ReplicaUnderConstructionStorage;

/**
 * @author wmalik
 *
 */
@PersistenceCapable(table = ReplicaUnderConstructionStorage.TABLE_NAME)
public interface ReplicaUcTable {

  @PrimaryKey
  @Column(name = ReplicaUnderConstructionStorage.BLOCK_ID)
  long getBlockId();

  void setBlockId(long blkid);

  @PrimaryKey
  @Column(name = ReplicaUnderConstructionStorage.STORAGE_ID)
  @Index(name = "idx_datanodeStorage")
  String getStorageId();

  void setStorageId(String id);

  @Column(name = ReplicaUnderConstructionStorage.REPLICA_INDEX)
  int getIndex();

  void setIndex(int index);

  @Column(name = ReplicaUnderConstructionStorage.STATE)
  int getState();

  void setState(int state);
}
