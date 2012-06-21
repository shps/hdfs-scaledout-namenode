/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ExcessReplicaStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
@PersistenceCapable(table = ExcessReplicaStorage.TABLE_NAME)
public interface ExcessReplicaTable {

  @PrimaryKey
  @Column(name = ExcessReplicaStorage.BLOCK_ID)
  long getBlockId();

  void setBlockId(long storageId);

  @PrimaryKey
  @Column(name = ExcessReplicaStorage.STORAGE_ID)
  String getStorageId();

  void setStorageId(String storageId);
}
