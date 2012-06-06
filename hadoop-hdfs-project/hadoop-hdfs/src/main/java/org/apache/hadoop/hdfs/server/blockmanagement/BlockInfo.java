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
import java.util.Comparator;
import java.util.List;


import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeHelper;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import se.sics.clusterj.BlockInfoTable;

/**
 * Internal class for block metadata.
 */
public class BlockInfo extends Block {

  public static enum Order implements Comparator<BlockInfo> {

    ByBlockIndex() {

      public int compare(BlockInfo o1, BlockInfo o2) {
        if (o1.getBlockIndex() < o2.getBlockIndex()) {
          return -1;
        } else {
          return 1;
        }
      }
    },
    ByGenerationStamp() {

      public int compare(BlockInfo o1, BlockInfo o2) {
        if (o1.getGenerationStamp() < o2.getGenerationStamp()) {
          return -1;
        } else {
          return 1;
        }
      }
    };

    public abstract int compare(BlockInfo o1, BlockInfo o2);

    public Comparator acsending() {
      return this;
    }

    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }
  private INodeFile inode;
  private List<Replica> replicas;
  /**
   * For implementing {@link LightWeightGSet.LinkedElement} interface
   */
  private int blockIndex = -1; //added for KTHFS
  private long timestamp = 1;
  
  protected long inodeId = -1;
  
  public BlockInfo(Block blk) {
    super(blk);
    if (blk instanceof BlockInfo) {
      this.inode = ((BlockInfo) blk).inode;
    }
    
  }

  public INodeFile getINode() {
    if (inode == null) {
      inode = (INodeFile) INodeHelper.getINode(inodeId);
    }

    return inode;
  }

  public void setINodeId(long id) {
    this.inodeId = id;
  }
  
  public void setINode(INodeFile inode) {
    this.inode = inode;
  }

  public List<Replica> getReplicas() {
    if (replicas == null) {
      replicas = EntityManager.getInstance().findReplicasByBlockId(getBlockId());
    }

    return replicas;
  }

  /**
   * Adds new replica for this block.
   */
  public Replica addReplica(DatanodeDescriptor dn) {
    if (hasReplicaIn(dn.getStorageID())) {
      return null;
    }

    Replica replica = new Replica(getBlockId(), dn.getStorageID(), getReplicas().size());
    replicas.add(replica);
    dn.increamentBlocks();
    return replica;
  }

  /**
   * removes a replica of this block related to storageId
   *
   * @param storageId
   * @return
   */
  public Replica removeReplica(DatanodeDescriptor dn) {
    Replica replica = null;
    int index = -1;
    
    for (int i = 0; i < getReplicas().size(); i++) {
      if (replicas.get(i).getStorageId().equals(dn.getStorageID())) {
        index = i;
        break;
      }
    }
    
    if (index >= 0) {
      replica = replicas.remove(index);
      dn.decrementBlocks();
      
      for (int i = index; i < replicas.size(); i++) {
        Replica r1 = replicas.get(i);
        r1.setIndex(i);
        EntityManager.getInstance().persist(r1);
      }
    }
    
    return replica;

  }

  boolean hasReplicaIn(String storageId) {
    for (Replica replica : getReplicas()) {
      if (replica.getStorageId().equals(storageId)) {
        return true;
      }
    }

    return false;
  }

  /**
   * BlockInfo represents a block that is not being constructed. In order to
   * start modifying the block, the BlockInfo should be converted to {@link BlockInfoUnderConstruction}.
   *
   * @return {@link BlockUCState#COMPLETE}
   */
  public BlockUCState getBlockUCState() {
    return BlockUCState.COMPLETE;
  }

  /**
   * Is this block complete?
   *
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  /**
   * Convert a complete block to an under construction block.
   *
   * @return BlockInfoUnderConstruction - an under construction block.
   * @throws IOException
   */
  public BlockInfoUnderConstruction convertToBlockUnderConstruction(
          BlockUCState s, DatanodeDescriptor[] targets, boolean isTransactional) throws IOException {
    if (isComplete()) {
      BlockInfoUnderConstruction bUc = new BlockInfoUnderConstruction(this);
      bUc.setBlockUCState(s);
      bUc.setExpectedLocations(targets, isTransactional);
      return bUc;
    }
    // the block is already under construction
    BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets, isTransactional);
    return ucBlock;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  /*
   * added for KTHFS
   */
  public int getBlockIndex() {
    return this.blockIndex;
  }
  /*
   * added for KTHFS
   */

  public void setBlockIndex(int bindex) {
    this.blockIndex = bindex;
  }

  /*
   * added for KTHFS
   */
  public long getTimestamp() {
    return this.timestamp;
  }
  /*
   * added for KTHFS
   */

  public void setTimestamp(long ts) {
    this.timestamp = ts;
  }

  public void toPersistable(BlockInfoTable persistable) {
  }
}