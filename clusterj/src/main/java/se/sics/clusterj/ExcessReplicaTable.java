/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
@PersistenceCapable(table = "InvalidateBlocks")
public interface ExcessReplicaTable {

  @PrimaryKey
  @Column(name = "storageId")
  String getStorageId();

  void setStorageId(String storageId);

  @PrimaryKey
  @Column(name = "blockId")
  long getBlockId();

  void setBlockId(long storageId);
}
