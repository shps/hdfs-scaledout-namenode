/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ReplicaUnderConstructionFinder;

/**
 * Represents a block that is currently being constructed.<br> This is usually
 * the last block of a file opened for write or append.
 */
public class BlockInfoUnderConstruction extends BlockInfo {

  /**
   * Block state. See {@link BlockUCState}
   */
  private BlockUCState blockUCState;
  /**
   * Block replicas as assigned when the block was allocated. This defines the
   * pipeline order.
   */
  private List<ReplicaUnderConstruction> expectedReplicas;
  /**
   * A data-node responsible for block recovery.
   */
  private int primaryNodeIndex = -1;
  /**
   * The new generation stamp, which this block will have after the recovery
   * succeeds. Also used as a recovery id to identify the right recovery if any
   * of the abandoned recoveries re-appear.
   */
  private long blockRecoveryId = 0;

  /**
   * Should only be called by BlocksHelper
   *
   * @param recoveryId
   */
  public void setBlockRecoveryId(long recoveryId) {
    this.blockRecoveryId = recoveryId;
  }

  /**
   * Should only be called by BlocksHelper
   *
   * @param nodeIndex
   */
  public void setPrimaryNodeIndex(int nodeIndex) {
    this.primaryNodeIndex = nodeIndex;
  }

  public int getPrimaryNodeIndex() {
    return this.primaryNodeIndex;
  }

  /**
   * Create block and set its state to
   * {@link BlockUCState#UNDER_CONSTRUCTION}.
   *
   * @throws IOException
   */
  public BlockInfoUnderConstruction(Block blk) {
    super(blk);
    this.blockUCState = BlockUCState.UNDER_CONSTRUCTION;
  }

  /**
   * Convert an under construction block to a complete block.
   *
   * @return BlockInfo - a complete block.
   * @throws IOException if the state of the block (the generation stamp and the
   * length) has not been committed by the client or it does not have at least a
   * minimal number of replicas reported from data-nodes.
   */
  BlockInfo convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != BlockUCState.COMPLETE :
            "Trying to convert a COMPLETE block";
    if (getBlockUCState() != BlockUCState.COMMITTED) {
      throw new IOException(
              "Cannot complete block: block has not been COMMITTED by the client");
    }
    return new BlockInfo(this);
  }

  /**
   * Create array of expected replica locations (as has been assigned by
   * chooseTargets()).
   *
   * @throws IOException
   */
  public List<ReplicaUnderConstruction> getExpectedReplicas() {
    if (expectedReplicas == null) {
      expectedReplicas = 
              (List<ReplicaUnderConstruction>) EntityManager.getInstance().findList(ReplicaUnderConstructionFinder.ByBlockId, getBlockId());
    }
    Collections.sort(expectedReplicas, ReplicaUnderConstruction.Order.ByIndex);
    return expectedReplicas;
  }

  public ReplicaUnderConstruction addExpectedReplica(String storageId, ReplicaState rState) {
    if (hasExpectedReplicaIn(storageId))
      return null;
    
    ReplicaUnderConstruction replica = new ReplicaUnderConstruction(rState, storageId, getBlockId(), getExpectedReplicas().size());
    getExpectedReplicas().add(replica);
    return replica;
  }

  private boolean hasExpectedReplicaIn(String storageId) {
    for (IndexedReplica replica : getExpectedReplicas()) {
      if (replica.getStorageId().equals(storageId)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Return the state of the block under construction.
   *
   * @see BlockUCState
   */
  @Override // BlockInfo
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  public void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  /**
   * Get block recovery ID
   */
  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /**
   * Commit block's length and generation stamp as reported by the client. Set
   * block state to {@link BlockUCState#COMMITTED}.
   */
  public void commitBlock(long reportedNumBytes, long reportedGenStamp) {
    blockUCState = BlockUCState.COMMITTED;
    setNumBytes(reportedNumBytes);
    setGenerationStamp(reportedGenStamp);
  }

  /**
   * Initialize lease recovery for this block. Find the first alive data-node
   * starting from the previous primary and make it primary.
   *
   * @throws IOException
   */
  public void initializeBlockRecovery(long recoveryId, DatanodeManager datanodeMgr, boolean isTransactional) throws IOException {

    setBlockUCState(BlockUCState.UNDER_RECOVERY);

    blockRecoveryId = recoveryId; //FIXME: this should be either persisted to database / or stored globally

    if (getExpectedReplicas().isEmpty()) {
      NameNode.stateChangeLog.warn("BLOCK*"
              + " INodeFileUnderConstruction.initLeaseRecovery:"
              + " No blocks found, lease removed.");
    }

    int previous = primaryNodeIndex; //FIXME: this should be either persisted to database / or stored globally

    for (int i = 1; i <= getExpectedReplicas().size(); i++) {
      int j = (previous + i) % getExpectedReplicas().size();
      ReplicaUnderConstruction replica = getExpectedReplicas().get(j);
      DatanodeDescriptor datanode = datanodeMgr.getDatanodeByStorageId(replica.getStorageId());
      if (datanode.isAlive) { //FIXME
        primaryNodeIndex = j;

        datanode.addBlockToBeRecovered(this);
        NameNode.stateChangeLog.info("BLOCK* " + this
                + " recovery started, primary=" + datanode);
        return;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(super.toString());
    b.append("{blockUCState=").append(blockUCState).append(", primaryNodeIndex=").append(primaryNodeIndex).append(", replicas=").append(getExpectedReplicas().toArray()).append("}");
    return b.toString();
  }
}
