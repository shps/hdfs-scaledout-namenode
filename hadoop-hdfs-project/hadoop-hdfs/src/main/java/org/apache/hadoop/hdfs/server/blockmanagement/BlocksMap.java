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
import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksHelper;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.hdfs.util.GSetDB;
import org.apache.hadoop.hdfs.util.LightWeightGSet;
import org.mortbay.log.Log;
import sun.nio.cs.ext.ISCII91;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes INode it belongs to and
 * the datanodes that store the block.
 */
class BlocksMap {
  private static class NodeIterator implements Iterator<DatanodeDescriptor> {
    private BlockInfo blockInfo;
    private int nextIdx = 0;
      
    NodeIterator(BlockInfo blkInfo) {
      this.blockInfo = blkInfo;
    }

    public boolean hasNext() {
    	//NameNode.LOG.debug("blockInfo=" + blockInfo);
    	//NameNode.LOG.debug("bi.getCapacity()="+ blockInfo.getCapacity());
    	//NameNode.LOG.debug("bi.getDatanode(nextIdx)="+ blockInfo.getDatanode(nextIdx));
      return blockInfo != null && nextIdx < blockInfo.getCapacity()
              && blockInfo.getDatanode(nextIdx) != null;
    }

    public DatanodeDescriptor next() {
      return blockInfo.getDatanode(nextIdx++);
    }

    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  /** Constant {@link LightWeightGSet} capacity. */
  private final int capacity;
  
  BlocksMap(final float loadFactor) {
    this.capacity = 0;
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns Iterator that iterates through the nodes the block belongs to.
   */
  Iterator<DatanodeDescriptor> nodeIterator(BlockInfo storedBlock) {
    return new NodeIterator(storedBlock);
  }


 }
