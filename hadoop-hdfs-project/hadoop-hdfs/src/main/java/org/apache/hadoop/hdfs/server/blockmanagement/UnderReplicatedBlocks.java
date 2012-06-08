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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicaBlocksHelper;

/** Keep track of under replication blocks.
 * Blocks have replication priority, with priority 0 indicating the highest
 * Blocks have only one replicas has the highest
 */
class UnderReplicatedBlocks implements Iterable<Block> {
  static final int LEVEL = 5;
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;
  // private final List<NavigableSet<Block>> priorityQueues  = new ArrayList<NavigableSet<Block>>();
      
  /** Create an object. */
  UnderReplicatedBlocks() {
//    for(int i=0; i<LEVEL; i++) {
//      priorityQueues.add(new TreeSet<Block>());
//    }
  }

  /**
   * Empty the queues.
   */
  void clear(boolean isTransactional) {
    UnderReplicaBlocksHelper.removeAll(isTransactional);
//    for(int i=0; i<LEVEL; i++) {
//      priorityQueues.get(i).clear();
//    }
  }

  /** Return the total number of under replication blocks */
  synchronized int size() {
    return UnderReplicaBlocksHelper.getTotalBlockCount();
//    int size = 0;
//    for (int i=0; i<LEVEL; i++) {
//      size += priorityQueues.get(i).size();
//    }
//    return size;
  }

  /** Return the number of under replication blocks excluding corrupt blocks */
  synchronized int getUnderReplicatedBlockCount() {
    return UnderReplicaBlocksHelper.getUnderReplicatedBlockCount(QUEUE_WITH_CORRUPT_BLOCKS);
//    int size = 0;
//    for (int i=0; i<QUEUE_WITH_CORRUPT_BLOCKS; i++) {
//      size += priorityQueues.get(i).size();
//    }
//    return size;
  }
  
  /** Return the number of corrupt blocks */
  synchronized int getCorruptBlockSize() {
    return UnderReplicaBlocksHelper.getCorruptedBlockSize(QUEUE_WITH_CORRUPT_BLOCKS);
    // return priorityQueues.get(QUEUE_WITH_CORRUPT_BLOCKS).size();
  }
  
  /** Check if a block is in the neededReplication queue */
  synchronized boolean contains(Block block) {
    return UnderReplicaBlocksHelper.contains(block.getBlockId());
//    for(NavigableSet<Block> set : priorityQueues) {
//      if(set.contains(block)) { return true; }
//    }
//    return false;
  }
      
  /** Return the priority of a block
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  private int getPriority(Block block, 
                          int curReplicas, 
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
                           int expectedReplicas,
                           boolean isTransactional) {
    assert curReplicas >= 0 : "Negative replicas!";
    int priLevel = getPriority(block, curReplicas, decomissionedReplicas,
                               expectedReplicas);
    //if(priLevel != LEVEL && priorityQueues.get(priLevel).add(block)) {
    if(priLevel != LEVEL && UnderReplicaBlocksHelper.add(priLevel, block.getBlockId(), isTransactional)) {
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
                              int oldExpectedReplicas, 
                              boolean isTransactional) {
    int priLevel = getPriority(block, oldReplicas, 
                               decommissionedReplicas,
                               oldExpectedReplicas);
    return remove(block, priLevel, isTransactional);
  }
      
  /** remove a block from a under replication queue given a priority*/
  boolean remove(Block block, int priLevel, boolean isTransactional) {
    //if(priLevel >= 0 && priLevel < LEVEL  && priorityQueues.get(priLevel).remove(block)) {
    if(priLevel >= 0 && priLevel < LEVEL  && UnderReplicaBlocksHelper.remove(priLevel, block.getBlockId(), isTransactional)) {
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
        //if(priorityQueues.get(i).remove(block)) {
        if(UnderReplicaBlocksHelper.remove(i, block.getBlockId(), isTransactional)) {
          if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
              "BLOCK* NameSystem.UnderReplicationBlock.remove: "
              + "Removing block " + block
              + " from priority queue "+ i);
          }
          return true;
        }
      }
    }
    return false;
  }
      
  /** update the priority level of a block */
  synchronized void update(Block block, int curReplicas, 
                           int decommissionedReplicas,
                           int curExpectedReplicas,
                           int curReplicasDelta, int expectedReplicasDelta,
                           boolean isTransactional) {
    int oldReplicas = curReplicas-curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
    int curPri = getPriority(block, curReplicas, decommissionedReplicas, curExpectedReplicas);
    int oldPri = getPriority(block, oldReplicas, decommissionedReplicas, oldExpectedReplicas);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " + 
        block +
        " curReplicas " + curReplicas +
        " curExpectedReplicas " + curExpectedReplicas +
        " oldReplicas " + oldReplicas +
        " oldExpectedReplicas  " + oldExpectedReplicas +
        " curPri  " + curPri +
        " oldPri  " + oldPri);
    }
    if(oldPri != LEVEL && oldPri != curPri) {
      //remove(block, oldPri);
      UnderReplicaBlocksHelper.remove(oldPri, block.getBlockId(), isTransactional);
    }
    //if(curPri != LEVEL && priorityQueues.get(curPri).add(block)) {
    if(curPri != LEVEL && UnderReplicaBlocksHelper.add(curPri, block.getBlockId(), isTransactional)) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.update:"
          + block
          + " has only "+curReplicas
          + " replicas and needs " + curExpectedReplicas
          + " replicas so is added to neededReplications"
          + " at priority level " + curPri);
      }
    }
  }

  /** returns an iterator of all blocks in a given priority queue */
  synchronized BlockIterator iterator(int level) {
    return new BlockIterator(level);
  }
    
  /** return an iterator of all the under replication blocks */
  public synchronized BlockIterator iterator() {
    return new BlockIterator();
  }
  
  //class BlockIterator implements Iterator<Block> {
  class BlockIterator implements Iterator<Block> {
    private int level;
    private boolean isIteratorForLevel = false;
    
    // KTHFS [J] Added here and removed from the outer class since we use the helper methods now to add / remove. We don't need a data structure
    // We only need the data structure (i.e. priorityQueues) for iterating (i.e. in this class)
    private List<NavigableSet<Block>> priorityQueues  = null;;
    //private List<Iterator<Block>> iterators = new ArrayList<Iterator<Block>>();
    private List<Iterator<Block>> iterators = new ArrayList<Iterator<Block>>();

    private BlockIterator() {
      // KTHFS [J] Get the latest under-replicated blocks from db
      priorityQueues = UnderReplicaBlocksHelper.getAllBlocks(LEVEL);
      
      level = 0;
      for (int i = 0; i < LEVEL; i++) {
        iterators.add(priorityQueues.get(i).iterator());
      }
    }

    private BlockIterator(int l) {
      // KTHFS [J] Get the latest under-replicated blocks from db
      priorityQueues = UnderReplicaBlocksHelper.getAllBlocks(LEVEL);

      level = l;
      isIteratorForLevel = true;
      iterators.add(priorityQueues.get(level).iterator());
    }

    private void update() {
      if (isIteratorForLevel)
        return;
      while(level< LEVEL-1 && !iterators.get(level).hasNext()) {
        level++;
      }
    }

    @Override
    public Block next() {
      if (isIteratorForLevel)
        return iterators.get(0).next();
      update();
      return iterators.get(level).next();
    }

    @Override
    public boolean hasNext() {
      if (isIteratorForLevel)
        return iterators.get(0).hasNext();
      update();
      return iterators.get(level).hasNext();
    }

    @Override
    public void remove() {
      if (isIteratorForLevel) 
        iterators.get(0).remove();
      else
        iterators.get(level).remove();
    }

    int getPriority() {
      return level;
    }
  }  
}
