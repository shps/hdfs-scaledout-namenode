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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;

import java.util.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/**
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are
 * corrupt. While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have 
 * expected number of good replicas.
 * Mapping: Block -> TreeSet<DatanodeDescriptor> 
 * 
 * KTHFS [J]: This data structure is also persisted to NDB. But we also store in this data structure for read operations (i.e. this is essentially  a cache)
 */

@InterfaceAudience.Private
public class CorruptReplicasMap{

  private EntityManager em = EntityManager.getInstance();
  //private SortedMap<Block, Collection<DatanodeDescriptor>> corruptReplicasMap =  new TreeMap<Block, Collection<DatanodeDescriptor>>();

  /**
   * Mark the block belonging to datanode as corrupt.
   *
   * @param blk Block to be added to CorruptReplicasMap
   * @param dn DatanodeDescriptor which holds the corrupt replica
   */
  public void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn, boolean isTransactional) {
    em.persist(new CorruptReplica(blk.getBlockId(), dn.getStorageID()));
    //CorruptReplicasHelper.addToCorruptReplicas(blk, dn, isTransactional);
    NameNode.stateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   blk.getBlockName() +
                                   " added as corrupt on " + dn.getName() +
                                   " by " + Server.getRemoteIp());
  }
  /*
  public void addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn, boolean isTransactional) {
    Collection<DatanodeDescriptor> ` = getNodes(blk);
    
    // First time entry for this block
    if (nodes == null) {
      nodes = new TreeSet<DatanodeDescriptor>();
      CorruptReplicasHelper.addToCorruptReplicas(blk, dn, isTransactional);
      corruptReplicasMap.put(blk, nodes);
    }
    if (!nodes.contains(dn)) {
      
      // New corrupt replica for this block (some corrupt replicas already exist)
      CorruptReplicasHelper.addToCorruptReplicas(blk, dn, isTransactional);
      nodes.add(dn);
      NameNode.stateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   blk.getBlockName() +
                                   " added as corrupt on " + dn.getName() +
                                   " by " + Server.getRemoteIp());
    } else {
      NameNode.stateChangeLog.info("BLOCK NameSystem.addToCorruptReplicasMap: "+
                                   "duplicate requested for " + 
                                   blk.getBlockName() + " to add as corrupt " +
                                   "on " + dn.getName() +
                                   " by " + Server.getRemoteIp());
    }
  }
*/
  /**
   * Remove Block from CorruptBlocksMap
   *
   * @param blk Block to be removed
   */
  void removeFromCorruptReplicasMap(Block blk, boolean isTransactional) {
    Collection<DatanodeDescriptor> datanodes = em.findCorruptReplica(blk.getBlockId());
    for(DatanodeDescriptor dn : datanodes) {
      em.remove(new CorruptReplica(blk.getBlockId(), dn.getStorageID()));  
    }
    //CorruptReplicasHelper.removeFromCorruptReplicas(blk, isTransactional);
  }
  /*
  void removeFromCorruptReplicasMap(Block blk, boolean isTransactional) {
    if (corruptReplicasMap != null) {
      CorruptReplicasHelper.removeFromCorruptReplicas(blk, isTransactional);
      corruptReplicasMap.remove(blk);
    }
  }
*/
  /**
   * Remove the block at the given datanode from CorruptBlockMap
   * @param blk block to be removed
   * @param datanode datanode where the block is located
   * @return true if the removal is successful; 
             false if the replica is not in the map
   */ 
  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode, boolean isTransactional) {
    em.remove(new CorruptReplica(blk.getBlockId(), datanode.getStorageID())); 
    return true;
    //return (CorruptReplicasHelper.removeFromCorruptReplicas(blk, datanode, isTransactional) > 0) ? true : false;
  }
  /*
  boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor datanode, boolean isTransactional) {
    Collection<DatanodeDescriptor> datanodes = corruptReplicasMap.get(blk);
    if (datanodes==null)
      return false;
    if (datanodes.remove(datanode)) { // remove the replicas
      CorruptReplicasHelper.removeFromCorruptReplicas(blk, datanode, isTransactional);
      if (datanodes.isEmpty()) {
        // remove the block if there is no more corrupted replicas
        // KTHFS: No need to remove from table because rows would no longer exist for this block
        corruptReplicasMap.remove(blk);
      }
      return true;
    }
    return false;
  }
*/   

  /**
   * Get Nodes which have corrupt replicas of Block
   * 
   * @param blk Block for which nodes are requested
   * @return collection of nodes. Null if does not exists
   */
  Collection<DatanodeDescriptor> getNodes(Block blk) {
    return em.findCorruptReplica(blk.getBlockId());
    //return corruptReplicasMap.get(blk);
    //return CorruptReplicasHelper.getNodes(blk);
  }

  /**
   * Check if replica belonging to Datanode is corrupt
   *
   * @param blk Block to check
   * @param node DatanodeDescriptor which holds the replica
   * @return true if replica is corrupt, false if does not exists in this map
   */
  boolean isReplicaCorrupt(Block blk, DatanodeDescriptor node) {
    return (em.findCorruptReplica(blk.getBlockId(), node.getStorageID()) == null? false : true);
    //Collection<DatanodeDescriptor> nodes = getNodes(blk);
    //return ((nodes != null) && (nodes.contains(node)));
    //return CorruptReplicasHelper.isReplicaCorrupt(blk, node);
  }

  public int numCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> datanodes = em.findCorruptReplica(blk.getBlockId());
    return (datanodes == null) ? 0 : datanodes.size();
    //Collection<DatanodeDescriptor> nodes = getNodes(blk);
    //return (nodes == null) ? 0 : nodes.size();
  }
  
  public int size() {
    Collection<CorruptReplica> datanodes = em.findAllCorruptBlocks();
    return (datanodes == null) ? 0 : datanodes.size();
    //return corruptReplicasMap.size();
    //return CorruptReplicasHelper.getCorruptBlocks().size();
  }

  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks 
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId 
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks Number of block ids to return.
   *  0 <= numExpectedBlocks <= 100
   * @param startingBlockId Block id from which to start. If null, start at
   *  beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   *
   */
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    //Iterator<Block> blockIt = corruptReplicasMap.keySet().iterator();
    //Iterator<Long> blockIt = CorruptReplicasHelper.getCorruptBlocks().iterator();
    SortedSet<Long> blocks = new TreeSet<Long>();
    Iterator<CorruptReplica> iteratorReplica = em.findAllCorruptBlocks().iterator();
    while(iteratorReplica.hasNext()) {
      blocks.add(iteratorReplica.next().getBlockId());  
    }
    Iterator<Long> blockIt = blocks.iterator();
    
    
    // if the starting block id was specified, iterate over keys until
    // we find the matching block. If we find a matching block, break
    // to leave the iterator on the next block after the specified block. 
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        long b = blockIt.next();
        if (b == startingBlockId) {
          isBlockFound = true;
          break; 
        }
      }
      
      if (!isBlockFound) {
        return null;
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<Long>();

    // append up to numExpectedBlocks blockIds to our list
    for(int i=0; i<numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next());
    }
    
    long[] ret = new long[corruptReplicaBlockIds.size()];
    for(int i=0; i<ret.length; i++) {
      ret[i] = corruptReplicaBlockIds.get(i);
    }
    
    return ret;
  }  
  /*
  long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    if (numExpectedBlocks < 0 || numExpectedBlocks > 100) {
      return null;
    }
    
    Iterator<Block> blockIt = corruptReplicasMap.keySet().iterator();
    
    // if the starting block id was specified, iterate over keys until
    // we find the matching block. If we find a matching block, break
    // to leave the iterator on the next block after the specified block. 
    if (startingBlockId != null) {
      boolean isBlockFound = false;
      while (blockIt.hasNext()) {
        Block b = blockIt.next();
        if (b.getBlockId() == startingBlockId) {
          isBlockFound = true;
          break; 
        }
      }
      
      if (!isBlockFound) {
        return null;
      }
    }

    ArrayList<Long> corruptReplicaBlockIds = new ArrayList<Long>();

    // append up to numExpectedBlocks blockIds to our list
    for(int i=0; i<numExpectedBlocks && blockIt.hasNext(); i++) {
      corruptReplicaBlockIds.add(blockIt.next().getBlockId());
    }
    
    long[] ret = new long[corruptReplicaBlockIds.size()];
    for(int i=0; i<ret.length; i++) {
      ret[i] = corruptReplicaBlockIds.get(i);
    }
    
    return ret;
  }  
   * 
   */
}
