package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.InvalidatedBlockStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
@PersistenceCapable(table = InvalidatedBlockStorage.TABLE_NAME)
public interface InvalidateBlocksTable {

  @PrimaryKey
  @Column(name = InvalidatedBlockStorage.STORAGE_ID)
  String getStorageId();

  void setStorageId(String storageId);

  @PrimaryKey
  @Column(name = InvalidatedBlockStorage.BLOCK_ID)
  long getBlockId();

  void setBlockId(long storageId);

  @Column(name = InvalidatedBlockStorage.GENERATION_STAMP)
  long getGenerationStamp();

  void setGenerationStamp(long generationStamp);

  @Column(name = InvalidatedBlockStorage.GENERATION_STAMP)
  long getNumBytes();

  void setNumBytes(long numBytes);
}
