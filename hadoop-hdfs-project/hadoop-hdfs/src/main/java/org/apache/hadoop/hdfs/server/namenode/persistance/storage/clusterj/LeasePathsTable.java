package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeasePathStorage;

/**
 * This is a ClusterJ interface for interacting with the "LeasePaths" table
 * @author wmalik
 */
@PersistenceCapable(table = LeasePathStorage.TABLE_NAME)
public interface LeasePathsTable {

  @Column(name = LeasePathStorage.HOLDER_ID)
  int getHolderId();

  void setHolderId(int holder_id);

  @PrimaryKey
  @Column(name = LeasePathStorage.PATH)
  String getPath();

  void setPath(String path);
}
