/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Index;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaStorage;

/**
 *
 * @author wmalik
 */
@PersistenceCapable(table = IndexedReplicaStorage.TABLE_NAME)
public interface TripletsTable {

  @PrimaryKey
  @Column(name = IndexedReplicaStorage.BLOCK_ID)
  long getBlockId();

  void setBlockId(long bid);

  @PrimaryKey
  @Column(name = IndexedReplicaStorage.STORAGE_ID)
  @Index(name = "idx_datanodeStorage")
  String getStorageId();

  void setStorageId(String id);

  @Column(name = IndexedReplicaStorage.REPLICA_INDEX)
  int getIndex();

  void setIndex(int index);
}
