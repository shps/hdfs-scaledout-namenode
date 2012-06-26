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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/** Keep track of under replication blocks.
 * Blocks have replication priority, with priority 0 indicating the highest
 * Blocks have only one replicas has the highest
 */
class UnderReplicatedBlocks {
  static final int LEVEL = 5;
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;
  private EntityManager em = EntityManager.getInstance();
      
  /**
   * Empty the queues.
   */
  void clear() {
    em.removeAll(UnderReplicatedBlock.class);
  }

  /** Return the total number of under replication blocks */
  synchronized int size() {
    return em.countAllUnderReplicatedBlocks();
  }

  /** Return the number of under replication blocks excluding corrupt blocks */
  synchronized int getUnderReplicatedBlockCount() {
    return em.countUnderReplicatedBlocksLowerThanLevel(QUEUE_WITH_CORRUPT_BLOCKS);
  }
  
  /** Return the number of corrupt blocks */
  synchronized int getCorruptBlockSize() {
    return em.countUnderReplicatedBlocksByLevel(QUEUE_WITH_CORRUPT_BLOCKS);
  }
  
  /** Check if a block is in the neededReplication queue */
  synchronized boolean contains(Block block) {
    return em.findUnderReplicatedBlockByBlockId(block.getBlockId()) != null;
  }
      
  /** Return the priority of a block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  private int getPriority( int curReplicas, 
                          int decommissionedReplicas,
                          int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    if (curReplicas >= expectedReplicas) {
      return 3; // Block doesn't have enough racks
    } else if(curReplicas==0) {
      // If there are zero non-decommissioned replica but there are
      // some decommissioned replicas, then assign them highest priority
      if (decommissionedReplicas > 0) {
        return 0;
      }
      return QUEUE_WITH_CORRUPT_BLOCKS; // keep these blocks in needed replication.
    } else if(curReplicas==1) {
      return 0; // highest priority
    } else if(curReplicas*3<expectedReplicas) {
      return 1;
    } else {
      return 2;
    }
  }
      
  /** add a block to a under replication queue according to its priority
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  synchronized boolean add(
                           Block block,
                           int curReplicas, 
                           int decomissionedReplicas,
                           int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    int priLevel = getPriority(curReplicas, decomissionedReplicas,
                               expectedReplicas);
    
    if(priLevel != LEVEL) {
      em.persist(new UnderReplicatedBlock(priLevel, block.getBlockId()));
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.add:"
          + block
          + " has only "+curReplicas
          + " replicas and need " + expectedReplicas
          + " replicas so is added to neededReplications"
          + " at priority level " + priLevel);
      }
      return true;
    }
    return false;
  }

  /** remove a block from a under replication queue */
  synchronized boolean remove(Block block, 
                              int oldReplicas, 
                              int decommissionedReplicas,
                              int oldExpectedReplicas) {
    int priLevel = getPriority(oldReplicas, 
                               decommissionedReplicas,
                               oldExpectedReplicas);
    if(em.findUnderReplicatedBlockByBlockId(block.getBlockId()) != null) {
      return remove(block, priLevel);
    }
    else {
      return false;
    }
  }
      
  /** remove a block from a under replication queue given a priority*/
  boolean remove(Block block, int priLevel) {
    UnderReplicatedBlock urblock = em.findUnderReplicatedBlockByBlockId(block.getBlockId());
     if( urblock == null) {
       return false;
     }
    if(priLevel >= 0 && priLevel < LEVEL) {
      em.remove(urblock);
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.remove: "
          + "Removing block " + block
          + " from priority queue "+ priLevel);
      }
      return true;
    } else {
      // Try to remove the block from all queues if the block was
      // not found in the queue for the given priority level.
      for(int i=0; i<LEVEL; i++) {
        em.remove(new UnderReplicatedBlock(i, block.getBlockId()));
          if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
              "BLOCK* NameSystem.UnderReplicationBlock.remove: "
              + "Removing block " + block
              + " from priority queue "+ i);
          }
          return true;
        //}
      }
    }
    return false;
  }
      
  /** update the priority level of a block */
  synchronized void update(Block block, int curReplicas,
                           int decommissionedReplicas,
                           int curExpectedReplicas,
                           int curReplicasDelta, int expectedReplicasDelta) {
    int oldReplicas = curReplicas - curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas - expectedReplicasDelta;
    int curPri = getPriority(curReplicas, decommissionedReplicas, curExpectedReplicas);
    int oldPri = getPriority(oldReplicas, decommissionedReplicas, oldExpectedReplicas);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("UnderReplicationBlocks.update "
                                    + block
                                    + " curReplicas " + curReplicas
                                    + " curExpectedReplicas " + curExpectedReplicas
                                    + " oldReplicas " + oldReplicas
                                    + " oldExpectedReplicas  " + oldExpectedReplicas
                                    + " curPri  " + curPri
                                    + " oldPri  " + oldPri);
    }
    
    // Update the priority levels
    if ((oldPri != LEVEL && curPri != LEVEL)) {
      NameNode.stateChangeLog.debug("Updating replication for block "+block.getBlockId()+" by "+expectedReplicasDelta);
      UnderReplicatedBlock urb = em.findUnderReplicatedBlockByBlockId(block.getBlockId());
      if(urb == null)
        urb = new UnderReplicatedBlock(curPri, block.getBlockId());
      urb.setLevel(curPri);
      em.persist(urb);
    }
    
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
              "BLOCK* NameSystem.UnderReplicationBlock.update:"
              + block
              + " has only " + curReplicas
              + " replicas and needs " + curExpectedReplicas
              + " replicas so is added to neededReplications"
              + " at priority level " + curPri);
    }
  }

}
