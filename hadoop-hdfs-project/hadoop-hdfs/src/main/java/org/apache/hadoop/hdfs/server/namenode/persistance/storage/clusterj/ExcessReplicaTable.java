/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
@PersistenceCapable(table = "ExcessReplica")
public interface ExcessReplicaTable {

  @PrimaryKey
  @Column(name = "blockId")
  long getBlockId();

  void setBlockId(long storageId);

  @PrimaryKey
  @Column(name = "storageId")
  String getStorageId();

  void setStorageId(String storageId);
}
