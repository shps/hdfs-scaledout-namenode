package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeaseStorage;

/**
 * @author wmalik
 *
 * This is a ClusterJ interface for interacting with the "Lease" table
 *
 */
@PersistenceCapable(table = LeaseStorage.TABLE_NAME)
public interface LeaseTable {

  @PrimaryKey
  @Column(name = LeaseStorage.HOLDER)
  String getHolder();

  void setHolder(String holder);

  @Column(name = LeaseStorage.LAST_UPDATE)
  long getLastUpdate();

  void setLastUpdate(long last_upd);

  @Column(name = LeaseStorage.HOLDER_ID)
  int getHolderID();

  void setHolderID(int holder_id);
}
