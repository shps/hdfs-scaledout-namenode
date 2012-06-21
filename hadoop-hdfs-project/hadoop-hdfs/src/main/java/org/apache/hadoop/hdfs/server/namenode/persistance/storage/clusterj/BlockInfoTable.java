/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoStorage;

/**
 *
 * @author wmalik
 */
@PersistenceCapable(table = BlockInfoStorage.TABLE_NAME)
public interface BlockInfoTable {

  @PrimaryKey
  @Column(name = BlockInfoStorage.BLOCK_ID)
  long getBlockId();

  void setBlockId(long bid);

  @Column(name = BlockInfoStorage.BLOCK_INDEX)
  int getBlockIndex();

  void setBlockIndex(int idx);

  @Column(name = BlockInfoStorage.INODE_ID)
  @Index(name = "idx_inodeid")
  long getINodeID();

  void setINodeID(long iNodeID);

  @Column(name = BlockInfoStorage.NUM_BYTES)
  long getNumBytes();

  void setNumBytes(long numbytes);

  @Column(name = BlockInfoStorage.GENERATION_STAMP)
  long getGenerationStamp();

  void setGenerationStamp(long genstamp);

  @Column(name = BlockInfoStorage.BLOCK_UNDER_CONSTRUCTION_STATE)
  int getBlockUCState();

  void setBlockUCState(int BlockUCState);

  @Column(name = BlockInfoStorage.TIME_STAMP)
  long getTimestamp();

  void setTimestamp(long ts);

  @Column(name = BlockInfoStorage.PRIMARY_NODE_INDEX)
  int getPrimaryNodeIndex();

  void setPrimaryNodeIndex(int replication);

  @Column(name = BlockInfoStorage.BLOCK_RECOVERY_ID)
  long getBlockRecoveryId();

  void setBlockRecoveryId(long recoveryId);
}
