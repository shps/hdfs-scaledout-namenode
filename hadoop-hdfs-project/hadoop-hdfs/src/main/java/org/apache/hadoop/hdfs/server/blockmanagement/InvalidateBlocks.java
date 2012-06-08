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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/** 
 * Keeps a Collection for every named machine containing blocks
 * that have recently been invalidated and are thought to live
 * on the machine in question.
 */
@InterfaceAudience.Private
class InvalidateBlocks {

  private final DatanodeManager datanodeManager;
  private EntityManager em = EntityManager.getInstance();

  InvalidateBlocks(final DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  /** @return the number of blocks to be invalidated . */
  synchronized long numBlocks() {
    return em.countAllInvalidatedBlocks();
  }

  /** Does this contain the block which is associated with the storage? */
  synchronized boolean contains(final String storageID, final Block block) {
    return em.findInvalidatedBlockByPK(storageID, block.getBlockId()) != null;
  }

  /**
   * Add a block to the block collection
   * which will be invalidated on the specified datanode.
   */
  synchronized void add(final Block block, final DatanodeInfo datanode,
          final boolean log) {

    em.persist(new InvalidatedBlock(datanode.getStorageID(), block.getBlockId()));

    if (log) {
      NameNode.stateChangeLog.info("BLOCK* " + getClass().getSimpleName()
              + ": add " + block + " to " + datanode.getName());
    }
  }

  /** Remove a storage from the invalidatesSet */
  synchronized void remove(final String storageID) {
    //[H] ClusterJ limitation: no delete op using where clause
    List<InvalidatedBlock> invBlocks = em.findInvalidatedBlocksByStorageId(storageID);
    if (invBlocks != null)
      for (InvalidatedBlock invBlock : invBlocks) {
        if (invBlock != null)
          em.remove(invBlock);
    }
  }

  /** Remove the block from the specified storage. */
  synchronized void remove(final String storageID, final Block block) {
    em.remove(new InvalidatedBlock(storageID, block.getBlockId()));
  }

  /** Print the contents to out. */
  synchronized void dump(final PrintWriter out) {
    Map<String, List<InvalidatedBlock>> node2blocks = em.findAllInvalidatedBlocks();
    final int size = node2blocks.values().size();
    out.println("Metasave: Blocks " + em.countAllInvalidatedBlocks()
            + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }

    for (Map.Entry<String, List<InvalidatedBlock>> entry : node2blocks.entrySet()) {
      final List<InvalidatedBlock> invBlocks = entry.getValue();
      if (invBlocks.size() > 0) {
        //FIXME [H]: To dump properly it needs to get the block using blockid.
        out.println(datanodeManager.getDatanode(entry.getKey()).getName() + invBlocks);
      }
    }
  }

  /** @return a list of the storage IDs. */
  synchronized List<String> getStorageIDs() {
    Set<String> storageIds = em.findAllInvalidatedBlocks().keySet();
    if (storageIds != null)
      return new ArrayList<String>(storageIds);
    
    return new ArrayList<String>();
  }

  /** Invalidate work for the storage. */
  int invalidateWork(final String storageId) {
    final DatanodeDescriptor dn = datanodeManager.getDatanode(storageId);
    if (dn == null) {
      List<InvalidatedBlock> invBlocks = em.findInvalidatedBlocksByStorageId(storageId);
      if (invBlocks != null)
        for (InvalidatedBlock ib : invBlocks) {
          em.remove(ib);
      }

      return 0;
    }
    final List<Block> toInvalidate = invalidateWork(storageId, dn);
    if (toInvalidate == null) {
      return 0;
    }

    if (NameNode.stateChangeLog.isInfoEnabled()) {
      NameNode.stateChangeLog.info("BLOCK* " + getClass().getSimpleName()
              + ": ask " + dn.getName() + " to delete " + toInvalidate);
    }
    return toInvalidate.size();
  }

  private synchronized List<Block> invalidateWork(
          final String storageId, final DatanodeDescriptor dn) {
    final List<InvalidatedBlock> set = em.findInvalidatedBlocksByStorageId(storageId);
    if (set == null) {
      return null;
    }

    // # blocks that can be sent in one message is limited
    final int limit = datanodeManager.blockInvalidateLimit;
    final List<Block> toInvalidate = new ArrayList<Block>(limit);
    final Iterator<InvalidatedBlock> it = set.iterator();
    for (int count = 0; count < limit && it.hasNext(); count++) {
      Replica invBlock = it.next();
      //[H] there is no need to generationstamp and timestamp in the datanode for invalidated blocks
      toInvalidate.add(new Block(invBlock.getBlockId())); 
      em.remove(invBlock);
    }

    dn.addBlocksToBeInvalidated(toInvalidate);
    return toInvalidate;
  }
}
