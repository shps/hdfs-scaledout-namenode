/**
 *
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/**
 * @author wmalik
 *
 */
@PersistenceCapable(table = "ReplicaUc")
public interface ReplicaUcTable {

  @PrimaryKey
  @Column(name = "blockId")
  long getBlockId();

  void setBlockId(long blkid);

  @PrimaryKey
  @Column(name = "storageId")
  @Index(name = "idx_datanodeStorage")
  String getStorageId();

  void setStorageId(String id);

  @Column(name = "indx")
  int getIndex();

  void setIndex(int index);

  @Column(name = "state")
  int getState();

  void setState(int state);
}
