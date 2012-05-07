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
  
  private GSetDB<Block, BlockInfo> blocks;

  BlocksMap(final float loadFactor) {
    this.capacity = computeCapacity();
    this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity);
  }

  /**
   * Let t = 2% of max memory.
   * Let e = round(log_2 t).
   * Then, we choose capacity = 2^e/(size of reference),
   * unless it is outside the close interval [1, 2^30].
   */
  private static int computeCapacity() {
    //VM detection
    //See http://java.sun.com/docs/hotspot/HotSpotFAQ.html#64bit_detection
    final String vmBit = System.getProperty("sun.arch.data.model");

    //2% of max memory
    final double twoPC = Runtime.getRuntime().maxMemory()/50.0;

    //compute capacity
    final int e1 = (int)(Math.log(twoPC)/Math.log(2.0) + 0.5);
    final int e2 = e1 - ("32".equals(vmBit)? 2: 3);
    final int exponent = e2 < 0? 0: e2 > 30? 30: e2;
    final int c = 1 << exponent;

    LightWeightGSet.LOG.info("VM type       = " + vmBit + "-bit");
    LightWeightGSet.LOG.info("2% max memory = " + twoPC/(1 << 20) + " MB");
    LightWeightGSet.LOG.info("capacity      = 2^" + exponent
        + " = " + c + " entries");
    return c;
  }

  void close() {
      //FIXME:kamal, It is not necessary, sice it makes the client requests get 
      //NullPointerException when the MiniDFSCluster is going to get shutdown which 
      //causes junit to stop running tests.
      //blocks = null;
  }

  INodeFile getINode(Block b) throws IOException {
    BlockInfo info = blocks.get(b);
    return (info != null) ? info.getINode() : null;
  }

  /**
   * Add block b belonging to the specified file inode to the map.
   * @throws IOException 
   */
  BlockInfo addINode(BlockInfo b, INodeFile iNode, boolean isTransactional) throws IOException {
    BlockInfo info = blocks.get(b);
    if (info != b) {
      info = b;
      blocks.put(info, isTransactional);
    }
    info.setINode(iNode, isTransactional);
    return info;
  }
  
  
  /**
   * KTHFS
   */
  /*BlockInfo addINodeKth(BlockInfo b, INodeFile iNode) {
 
    blocks.getFromDB(null);
 
  }*/
  

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   * @throws IOException 
   */
  void removeBlock(Block block, boolean isTransactional) throws IOException {
    BlockInfo blockInfo = blocks.remove(block, isTransactional);
    if (blockInfo == null)
    {
      return;
    }
    blockInfo.setINode(null, isTransactional);
    for(int idx = blockInfo.numNodes()-1; idx >= 0; idx--) {
      DatanodeDescriptor dn = blockInfo.getDatanode(idx);
      dn.removeBlock(blockInfo, isTransactional); // remove from the list and wipe the location
    }
  }
  
  /** Returns the block object it it exists in the map. 
   * @throws IOException */
  BlockInfo getStoredBlock(Block b) throws IOException {
    return blocks.get(b);
  }

  /**
   * Searches for the block in the BlocksMap and 
   * returns Iterator that iterates through the nodes the block belongs to.
   * @throws IOException 
   */
  Iterator<DatanodeDescriptor> nodeIterator(Block b) throws IOException {
    return nodeIterator(blocks.get(b));
  }

  /**
   * For a block that has already been retrieved from the BlocksMap
   * returns Iterator that iterates through the nodes the block belongs to.
   */
  Iterator<DatanodeDescriptor> nodeIterator(BlockInfo storedBlock) {
    return new NodeIterator(storedBlock);
  }

  /** counts number of containing nodes. Better than using iterator. 
   * @throws IOException */
  int numNodes(Block b) throws IOException {
    BlockInfo info = blocks.get(b);
    return info == null ? 0 : info.numNodes();
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   * @throws IOException 
   */
  boolean removeNode(Block b, DatanodeDescriptor node, boolean isTransactional) throws IOException {
    BlockInfo info = blocks.get(b);
    if (info == null)
      return false;

    // remove block from the data-node list and the node from the block info
    boolean removed = node.removeBlock(info, isTransactional);

    if (info.getDatanode(0) == null     // no datanodes left
              && info.getINode() == null) {  // does not belong to a file
      blocks.remove(b, isTransactional);  // remove block from the map
    }
    return removed;
  }

  int size() throws IOException {
    return blocks.size();
  }

  Iterable<BlockInfo> getBlocks() {
    return blocks;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  int getCapacity() {
    return capacity;
  }

  /**
   * Replace a block in the block map by a new block.
   * The new block and the old one have the same key.
   * @param newBlock - block for replacement
   * @return new block
   * @throws IOException 
   */
  BlockInfo replaceBlock(BlockInfo newBlock, boolean isTransactional) throws IOException {
	  
    BlockInfo currentBlock = blocks.get(newBlock);
    assert currentBlock != null : "the block if not in blocksMap";
    // replace block in data-node lists
    for(int idx = currentBlock.numNodes()-1; idx >= 0; idx--) {
      DatanodeDescriptor dn = currentBlock.getDatanode(idx);
      dn.replaceBlock(currentBlock, newBlock, isTransactional);
    }
    // replace block in the map itself
    blocks.put(newBlock, isTransactional);
    return newBlock;
  }
}
