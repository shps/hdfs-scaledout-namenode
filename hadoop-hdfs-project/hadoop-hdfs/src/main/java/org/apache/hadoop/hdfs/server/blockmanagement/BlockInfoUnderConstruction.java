/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.BlocksHelper;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ReplicaHelper;

/**
 * Represents a block that is currently being constructed.<br>
 * This is usually the last block of a file opened for write or append.
 */
public class BlockInfoUnderConstruction extends BlockInfo {
  /** Block state. See {@link BlockUCState} */
  private BlockUCState blockUCState;

  /**
   * Block replicas as assigned when the block was allocated.
   * This defines the pipeline order.
   */
  private List<ReplicaUnderConstruction> replicas;

  /** A data-node responsible for block recovery. */
  private int primaryNodeIndex = -1;

  /**
   * The new generation stamp, which this block will have
   * after the recovery succeeds. Also used as a recovery id to identify
   * the right recovery if any of the abandoned recoveries re-appear.
   */
  private long blockRecoveryId = 0;
  
  /** Should only be called by BlocksHelper
   * @param recoveryId
   */
  public void setBlockRecoveryId(long recoveryId) {
    this.blockRecoveryId = recoveryId;
  }
  
  /** Should only be called by BlocksHelper
   * @param nodeIndex
   */
  public void setPrimaryNodeIndex(int nodeIndex) {
    this.primaryNodeIndex = nodeIndex;
  }
  
  public int getPrimaryNodeIndex() {
    return this.primaryNodeIndex;
  }
  

  /**
   * ReplicaUnderConstruction contains information about replicas while
   * they are under construction.
   * The GS, the length and the state of the replica is as reported by 
   * the data-node.
   * It is not guaranteed, but expected, that data-nodes actually have
   * corresponding replicas.
   */
  public static class ReplicaUnderConstruction extends Block {
    private DatanodeDescriptor expectedLocation;
    private ReplicaState state;

    public ReplicaUnderConstruction(Block block,
                             DatanodeDescriptor target,
                             ReplicaState state) {
      super(block);
      this.expectedLocation = target;
      this.state = state;
    }

    /**
     * Expected block replica location as assigned when the block was allocated.
     * This defines the pipeline order.
     * It is not guaranteed, but expected, that the data-node actually has
     * the replica.
     */
    DatanodeDescriptor getExpectedLocation() {
      return expectedLocation;
    }

    /**
     * Get replica state as reported by the data-node.
     */
    ReplicaState getState() {
      return state;
    }

    /**
     * Set replica state.
     */
    void setState(ReplicaState s) {
      state = s;
    }

    /**
     * Is data-node the replica belongs to alive.
     */
    boolean isAlive() {
      return expectedLocation.isAlive;
    }

    @Override // Block
    public int hashCode() {
      return super.hashCode();
    }

    @Override // Block
    public boolean equals(Object obj) {
      // Sufficient to rely on super's implementation
      return (this == obj) || super.equals(obj);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      final StringBuilder b = new StringBuilder(getClass().getSimpleName());
      b.append("[")
       .append(expectedLocation)
       .append("|")
       .append(state)
       .append("]");
      return b.toString();
    }
  }

  /**
   * Create block and set its state to
   * {@link BlockUCState#UNDER_CONSTRUCTION}.
   * @throws IOException 
   */
  public BlockInfoUnderConstruction(Block blk, int replication) throws IOException {
    this(blk, replication, BlockUCState.UNDER_CONSTRUCTION, null);
  }

  /**
   * Create a block that is currently being constructed.
   * @throws IOException 
   */
  public BlockInfoUnderConstruction(Block blk, int replication,
                             BlockUCState state,
                             DatanodeDescriptor[] targets) throws IOException {
    super(blk, replication);
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "BlockInfoUnderConstruction cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(targets, false);
  }

  /**
   * Convert an under construction block to a complete block.
   * 
   * @return BlockInfo - a complete block.
   * @throws IOException if the state of the block 
   * (the generation stamp and the length) has not been committed by 
   * the client or it does not have at least a minimal number of replicas 
   * reported from data-nodes. 
   */
  BlockInfo convertToCompleteBlock() throws IOException {
    assert getBlockUCState() != BlockUCState.COMPLETE :
      "Trying to convert a COMPLETE block";
    if(getBlockUCState() != BlockUCState.COMMITTED)
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    return new BlockInfo(this);
  }

  /** Set expected locations 
   * @throws IOException */
  public void setExpectedLocations(DatanodeDescriptor[] targets, boolean isTransactional) throws IOException {
    int numLocations = targets == null ? 0 : targets.length;
    //this.replicas = new ArrayList<ReplicaUnderConstruction>(numLocations);
    for(int i = 0; i < numLocations; i++) {
      //replicas.add(
      //  new ReplicaUnderConstruction(this, targets[i], ReplicaState.RBW));
      ReplicaHelper.add(this.getBlockId(), targets[i], ReplicaState.RBW, isTransactional);
    }
  }

  /**
   * Create array of expected replica locations
   * (as has been assigned by chooseTargets()).
   * @throws IOException 
   */
  public DatanodeDescriptor[] getExpectedLocations() throws IOException {
    //int numLocations = replicas == null ? 0 : replicas.size();
    //DatanodeDescriptor[] locations = new DatanodeDescriptor[numLocations];
    //for(int i = 0; i < numLocations; i++)
    //  locations[i] = replicas.get(i).getExpectedLocation();
    //return locations;
    List<ReplicaUnderConstruction> replicasList = ReplicaHelper.getReplicas(this.getBlockId(), false);
    int numLocations = replicasList == null ? 0 : replicasList.size();
    DatanodeDescriptor[] locations = new DatanodeDescriptor[numLocations];
    for(int i = 0; i < numLocations; i++)
      locations[i] = replicasList.get(i).getExpectedLocation();
    return locations;
    
    
  }

  /** Get the number of expected locations */
  public int getNumExpectedLocations() {
    //return replicas == null ? 0 : replicas.size();
    return ReplicaHelper.size(this.getBlockId(), false);
  }

  /**
   * Return the state of the block under construction.
   * @see BlockUCState
   */
  @Override // BlockInfo
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  public void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  /** Get block recovery ID */
  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /**
   * Commit block's length and generation stamp as reported by the client.
   * Set block state to {@link BlockUCState#COMMITTED}.
   * @param block - contains client reported block length and generation 
   * @throws IOException if block ids are inconsistent.
   */
  void commitBlock(Block block, boolean isTransactional) throws IOException {
    if(getBlockId() != block.getBlockId())
      throw new IOException("Trying to commit inconsistent block: id = "
          + block.getBlockId() + ", expected id = " + getBlockId());
    blockUCState = BlockUCState.COMMITTED;
    this.set(getBlockId(), block.getNumBytes(), block.getGenerationStamp());
    BlocksHelper.updateBlockInfoInDB(this.getINode().getID(),this, isTransactional);
  }
  

  /**
   * Initialize lease recovery for this block.
   * Find the first alive data-node starting from the previous primary and
   * make it primary.
   * @throws IOException 
   */
  public void initializeBlockRecovery(long recoveryId, DatanodeManager datanodeMgr, boolean isTransactional) throws IOException {

    List<ReplicaUnderConstruction> replicasFromDB = ReplicaHelper.getReplicas(this.getBlockId(), isTransactional);
    
    setBlockUCState(BlockUCState.UNDER_RECOVERY); 
    BlocksHelper.updateBlockUCState(getBlockId(), BlockUCState.UNDER_RECOVERY, isTransactional);
    
    blockRecoveryId = recoveryId; //FIXME: this should be either persisted to database / or stored globally
    BlocksHelper.updateBlockRecoveryId(this.getBlockId(), blockRecoveryId, isTransactional);
    
    if (replicasFromDB.size() == 0) {
      NameNode.stateChangeLog.warn("BLOCK*"
        + " INodeFileUnderConstruction.initLeaseRecovery:"
        + " No blocks found, lease removed.");
    }

    int previous = primaryNodeIndex; //FIXME: this should be either persisted to database / or stored globally
    
    for(int i = 1; i <= replicasFromDB.size(); i++) {
      int j = (previous + i)%replicasFromDB.size();
      ReplicaUnderConstruction replica = replicasFromDB.get(j);
      DatanodeDescriptor datanodeFromDB = replica.getExpectedLocation();
      DatanodeDescriptor datanode = datanodeMgr.getDatanode(datanodeFromDB.getStorageID());
      if (datanode.isAlive) { //FIXME
        primaryNodeIndex = j;
        BlocksHelper.updatePrimaryNodeIndex(this.getBlockId(), primaryNodeIndex, isTransactional);
        
        //DatanodeDescriptor primary = replicasFromDB.get(j).getExpectedLocation(); //FIXME 
        //primary.addBlockToBeRecovered(this);
        datanode.addBlockToBeRecovered(this);
        NameNode.stateChangeLog.info("BLOCK* " + this
          + " recovery started, primary=" + datanode);
        return;
      }
    }
  }
  
//  public void initializeBlockRecoveryOld(long recoveryId) throws IOException {
//    //[W] setBlockUCState in DB
//    //[W] fetch replicas from DB
//    List<ReplicaUnderConstruction> replicasFromDB = ReplicaHelper.getReplicas(this.getBlockId(), false);
//    
//    setBlockUCState(BlockUCState.UNDER_RECOVERY);
//    blockRecoveryId = recoveryId; //FIXME: this should be either persisted to database / or stored globally 
//    if (replicasFromDB.size() == 0) {
//      NameNode.stateChangeLog.warn("BLOCK*"
//        + " INodeFileUnderConstruction.initLeaseRecovery:"
//        + " No blocks found, lease removed.");
//    }
//
//    int previous = primaryNodeIndex; //FIXME: this should be either persisted to database / or stored globally 
//    for(int i = 1; i <= replicasFromDB.size(); i++) {
//      int j = (previous + i)%replicasFromDB.size(); //FIXME
//      if (replicasFromDB.get(j).isAlive()) { //FIXME
//        primaryNodeIndex = j;
//        DatanodeDescriptor primary = replicasFromDB.get(j).getExpectedLocation(); //FIXME 
//        primary.addBlockToBeRecovered(this);
//        NameNode.stateChangeLog.info("BLOCK* " + this
//          + " recovery started, primary=" + primary);
//        return;
//      }
//    }
//  }

//  void addReplicaIfNotPresentOld(DatanodeDescriptor dn,
//                     Block block,
//                     ReplicaState rState) {
//    for(ReplicaUnderConstruction r : replicas)
//      if(r.getExpectedLocation() == dn)
//        return;
//    replicas.add(new ReplicaUnderConstruction(block, dn, rState));
//  }
  
  void addReplicaIfNotPresent(DatanodeDescriptor dn,
      Block block,
      ReplicaState rState, boolean isTransactional) throws IOException {
    
    List<ReplicaUnderConstruction> replicasFromDB = ReplicaHelper.getReplicas(this.getBlockId(), false);
    for(ReplicaUnderConstruction r : replicasFromDB)
      if(r.getExpectedLocation().getName().equals(dn.getName()))
        return;
    ReplicaHelper.add(this.getBlockId(), dn, rState, isTransactional);
  }

  @Override // BlockInfo
  // BlockInfoUnderConstruction participates in maps the same way as BlockInfo
  public int hashCode() {
    return super.hashCode();
  }

  @Override // BlockInfo
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj) || (this.getBlockId() == ((BlockInfoUnderConstruction)obj).getBlockId()); //FIXME: W
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(super.toString());
    b.append("{blockUCState=").append(blockUCState)
     .append(", primaryNodeIndex=").append(primaryNodeIndex)
     .append(", replicas=").append(replicas)
     .append("}");
    return b.toString();
  }
}
