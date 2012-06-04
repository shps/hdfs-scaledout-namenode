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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/**
 * I-node for closed file.
 */
public class INodeFile extends INode {

  static final FsPermission UMASK = FsPermission.createImmutable((short) 0111);
  /**
   * Number of bits for Block size
   */
  static final short BLOCKBITS = 48;
  /**
   * Header mask 64-bit representation
   * Format: [16 bits for replication][48 bits for PreferredBlockSize]
   */ 
  static final long HEADERMASK = 0xffffL << BLOCKBITS;
  protected long header;
  private List<BlockInfo> blocks = null;

  protected INodeFile(PermissionStatus permissions,
          short replication, long modificationTime,
          long atime, long preferredBlockSize) {
    super(permissions, modificationTime, atime);
    this.setReplication(replication);
    this.setPreferredBlockSize(preferredBlockSize);
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file
   *
   * @return block replication value
   */
  public short getReplication() {
    return (short) ((header & HEADERMASK) >> BLOCKBITS);
  }

  public void setReplication(short replication) {
    if (replication <= 0) {
      throw new IllegalArgumentException("Unexpected value for the replication");
    }
    header = ((long) replication << BLOCKBITS) | (header & ~HEADERMASK);
  }

  /**
   * [STATELESS] get header If you need preferred block size, or other
   * properties, select/set this value first.
   */
  public long getHeader() {
    return header;
  }

  /**
   * [STATELESS] set header
   */
  public void setHeader(long val) {
    header = val;
  }

  /**
   * Get preferred block size for the file
   *
   * @return preferred block size in bytes
   */
  public long getPreferredBlockSize() {
    return header & ~HEADERMASK;
  }

  public void setPreferredBlockSize(long preferredBlkSize) {
    if ((preferredBlkSize < 0) || (preferredBlkSize > ~HEADERMASK)) {
      throw new IllegalArgumentException("Unexpected value for the block size");
    }
    header = (header & HEADERMASK) | (preferredBlkSize & ~HEADERMASK);
  }

  /**
   * Get file blocks
   *
   * @return file blocks
   * @throws IOException
   */
  public List<BlockInfo> getBlocks() {
    if (blocks == null) {
      blocks = EntityManager.getInstance().findBlocksByInodeId(id);
    }
    
    Collections.sort(blocks, BlockInfo.Order.ByBlockIndex);
    return blocks;
  }
  
  public void setBlocks(List<BlockInfo> blocks) {
    this.blocks = blocks;
  }

  public void addBlock(BlockInfo block) {
    List<BlockInfo> blks = getBlocks();
    block.setBlockIndex(blks.size());
    blks.add(block);
  }
  
  public void removeBlock(BlockInfo block) throws Exception {
    List<BlockInfo> blks = getBlocks();
    int index = block.getBlockIndex();
    
    if (blks.get(index) != block)
      throw new Exception("BlockIndex mismatch");
    
    blks.remove(block);
    
    if (index != blks.size())
      for (int i = index; i < blocks.size(); i++) {
        blocks.get(i).setBlockIndex(i);
        EntityManager.getInstance().persist(blocks.get(i));
      }
  }
  
  public void setBlock(int index, BlockInfo block, boolean isTransactional) {
    List<BlockInfo> blks = getBlocks();
    
    block.setBlockIndex(index);
    blks.set(index, block);
    
    if (index < blks.size() - 1) {
      for (int i = index + 1; i < blks.size(); i++) {
        blks.get(i).setBlockIndex(i);
        EntityManager.getInstance().persist(blks.get(i));
      }
    }
      
  }
  
  /**
   * Set the {@link FsPermission} of this {@link INodeFile}. Since this is a
   * file, the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  @Override
  protected void setPermission(FsPermission permission) {
    super.setPermission(permission.applyUMask(UMASK));
  }

  @Override
  int collectSubtreeBlocksAndClear(List<Block> blocks, boolean isTransactional) {
    collectSubtreeBlocksAndClearNoDelete(blocks, isTransactional);
    INodeHelper.removeChild(super.id, isTransactional);
    return 1;
  }

  int collectSubtreeBlocksAndClearNoDelete(List<Block> v, boolean isTransactional) {

    parent = null;
    List<BlockInfo> tempList = new ArrayList<BlockInfo>(getBlocks());
    for (BlockInfo blk : tempList) {
      blocks.add(blk);
      blk.setINode(null);
      EntityManager.getInstance().remove(blk);
    }
    blocks.clear();

    return 1;
  }

  /**
   * {@inheritDoc}
   */
  long[] computeContentSummary(long[] summary) {
    summary[0] += computeFileSize(true);
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  /**
   * Compute file size. May or may not include BlockInfoUnderConstruction.
   */
  long computeFileSize(boolean includesBlockInfoUnderConstruction) {
    List<BlockInfo> blks = getBlocks();
    if (blks.isEmpty()) {
      return 0;
    }
    final int last = blks.size() - 1;
    //check if the last block is BlockInfoUnderConstruction
    long bytes =  blks.get(last) instanceof BlockInfoUnderConstruction
            && !includesBlockInfoUnderConstruction
            ? 0 : blks.get(last).getNumBytes();
    
    for (int i = 0; i < blks.size() - 1 ; i++) {
      bytes += blks.get(i).getNumBytes();
    }
      
    return bytes;
  }

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed() {
    List<BlockInfo> list = getBlocks();
    Block[] array = list.toArray(new Block[list.size()]);
    return diskspaceConsumed(array);
  }

  long diskspaceConsumed(Block[] blkArr) {
    long size = 0;
    if (blkArr == null) {
      return 0;
    }

    for (Block blk : blkArr) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /*
     * If the last block is being written to, use prefferedBlockSize rather than
     * the actual block size.
     */
    if (blkArr.length > 0 && blkArr[blkArr.length - 1] != null
            && isUnderConstruction()) {
      size += getPreferredBlockSize() - blkArr[blkArr.length - 1].getNumBytes();
    }
    return size * getReplication();
  }

  /**
   * Return the penultimate allocated block for this file.
   *
   * @throws IOException
   */
  BlockInfo getPenultimateBlock() throws IOException {
    if (getBlocks().size() <= 1) {
      return null;
    }
    return blocks.get(blocks.size() - 2);
  }

  /**
   * Get the last block of the file. Make sure it has the right type.
   */
  public BlockInfo getLastBlock() throws IOException {

    if (getBlocks().isEmpty()) {
      return null;
    }
    return blocks.get(blocks.size() - 1);
  }

}
