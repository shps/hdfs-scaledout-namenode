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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.PrintWriter;
import java.sql.Time;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 * *************************************************
 * PendingReplicationBlocks does the bookkeeping of all blocks that are getting
 * replicated.
 *
 * It does the following: 1) record blocks that are getting replicated at this
 * instant. 2) a coarse grain timer to track age of replication request 3) a
 * thread that periodically identifies replication-requests that never made it.
 *
 **************************************************
 */
class PendingReplicationBlocks {

  private static final Log LOG = BlockManager.LOG;
  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  //private long timeout = 5 * 60 * 1000;
  private long timeout = 2 * 60 * 1000;

  PendingReplicationBlocks(long timeoutPeriod) {
    if (timeoutPeriod > 0) {
      this.timeout = timeoutPeriod;
    }
  }

  /**
   * Add a block to the list of pending Replications
   */
  void add(Block block, int numReplicas) throws PersistanceException {
    PendingBlockInfo found = EntityManager.find(PendingBlockInfo.Finder.ByPKey, block.getBlockId());
    if (found == null) {
      found = new PendingBlockInfo(block.getBlockId(), now(), numReplicas);
      EntityManager.add(found);
    } else {
      found.incrementReplicas(numReplicas);
      found.setTimeStamp(now());
      EntityManager.update(found);
    }
  }

  /**
   * One replication request for this block has finished. Decrement the number
   * of pending replication requests for this block.
   */
  void remove(Block block) throws PersistanceException {
    PendingBlockInfo found = EntityManager.find(PendingBlockInfo.Finder.ByPKey, block.getBlockId());
    if (found != null && !isTimedout(found)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing pending replication for " + block);
      }
      found.decrementReplicas();
      if (found.getNumReplicas() <= 0) {
        EntityManager.remove(found);
      } else {
        EntityManager.update(found);
      }
    }
  }

  /**
   * The total number of blocks that are undergoing replication
   */
  int size() throws PersistanceException {
    List<PendingBlockInfo> pendingBlocks = (List<PendingBlockInfo>) EntityManager.findList(PendingBlockInfo.Finder.All); //TODO[H]: This can be improved.
    if (pendingBlocks != null) {
      int count = 0;
      for (PendingBlockInfo p : pendingBlocks) {
        if (!isTimedout(p)) {
          count++;
        }
      }
      return count;
    }
    return 0;
  }

  private boolean isTimedout(PendingBlockInfo pendingBlock) {
    if (now() - timeout > pendingBlock.getTimeStamp()) {
      return true;
    }

    return false;
  }

  /**
   * How many copies of this block is pending replication?
   */
  int getNumReplicas(Block block) throws PersistanceException {
    PendingBlockInfo found = EntityManager.find(PendingBlockInfo.Finder.ByPKey, block.getBlockId());
    if (found != null && !isTimedout(found)) {
      return found.getNumReplicas();
    }
    return 0;
  }

  /**
   * Returns a list of blocks that have timed out their replication requests.
   * Returns null if no blocks have timed out.
   */
  List<PendingBlockInfo> getTimedOutBlocks() throws PersistanceException {
    long timeLimit = now() - timeout;
    List<PendingBlockInfo> timedoutPendings = (List<PendingBlockInfo>) EntityManager.findList(PendingBlockInfo.Finder.ByTimeLimit, timeLimit);
    if (timedoutPendings == null || timedoutPendings.size() <= 0) {
      return null;
    }

    return timedoutPendings;
  }

  /**
   * Iterate through all items and print them.
   */
  void metaSave(PrintWriter out) throws PersistanceException {
    List<PendingBlockInfo> pendingBlocks = (List<PendingBlockInfo>) EntityManager.findList(PendingBlockInfo.Finder.All);
    if (pendingBlocks != null) {
      out.println("Metasave: Blocks being replicated: "
              + pendingBlocks.size());
      for (PendingBlockInfo pendingBlock : pendingBlocks) {
        if (!isTimedout(pendingBlock)) {
          BlockInfo bInfo = EntityManager.find(BlockInfo.Finder.ById, pendingBlock.getBlockId());
          out.println(bInfo
                  + " StartTime: " + new Time(pendingBlock.getTimeStamp())
                  + " NumReplicaInProgress: "
                  + pendingBlock.getNumReplicas());
        }
      }
    }
  }
}
