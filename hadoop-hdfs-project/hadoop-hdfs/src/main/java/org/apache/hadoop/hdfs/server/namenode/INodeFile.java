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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/**
 * I-node for closed file.
 */
public class INodeFile extends INode {

  public static final FsPermission UMASK = FsPermission.createImmutable((short) 0111);
  private short replication;
  private long preferredBlockSize;
  private List<BlockInfo> blocks = null;
  private boolean underConstruction = false;
  private String clientName;         // lease holder
  private String clientMachine;
  private DatanodeID clientNode; // if client is a cluster node too.

  public INodeFile(boolean underConstruction, PermissionStatus permissions,
          short replication, long modificationTime,
          long atime, long preferredBlockSize) {
    super(permissions, modificationTime, atime);
    this.underConstruction = underConstruction;
    this.replication = replication;
    this.preferredBlockSize = preferredBlockSize;
  }
  
  public INodeFile(boolean undercConstruction, PermissionStatus permissions, short replication,
                                 long preferredBlockSize, long modTime, String clientName, 
                                 String clientMachine, DatanodeID clientNode) {
    this(undercConstruction, permissions.applyUMask(UMASK), replication, modTime, modTime, preferredBlockSize);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }
  
  public INodeFile(boolean underConstruction, byte[] name,
                             short blockReplication,
                             long modificationTime,
                             long preferredBlockSize,
                             PermissionStatus perm,
                             String clientName,
                             String clientMachine,
                             DatanodeID clientNode) {
    this(underConstruction, perm, blockReplication, modificationTime, modificationTime,
          preferredBlockSize);
    this.name = name;
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  public short getReplication() {
    return replication;
  }

  public void setReplication(short replication) {
    this.replication = replication;
  }

  public long getPreferredBlockSize() {
    return preferredBlockSize;
  }

  public void setPreferredBlockSize(long preferredBlockSize) {
    this.preferredBlockSize = preferredBlockSize;
  }

  /**
   * Get file blocks
   *
   * @return file blocks
   * @throws IOException
   */
  public List<BlockInfo> getBlocks() {
    if (blocks == null) {
      blocks = (List<BlockInfo>) EntityManager.findList(BlockInfo.Finder.ByInodeId, id);
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

  public void removeBlock(BlockInfo block) {
    List<BlockInfo> blks = getBlocks();
    int index = block.getBlockIndex();

    blks.remove(block);

    if (index != blks.size()) {
      for (int i = index; i < blocks.size(); i++) {
        blocks.get(i).setBlockIndex(i);
        EntityManager.update(blocks.get(i));
      }
    }
  }

  public void setBlock(int index, BlockInfo block) {
    List<BlockInfo> blks = getBlocks();

    block.setBlockIndex(index);
    blks.set(index, block);

    if (index < blks.size() - 1) {
      for (int i = index + 1; i < blks.size(); i++) {
        blks.get(i).setBlockIndex(i);
        EntityManager.update(blks.get(i));
      }
    }

  }

  /**
   * Set the {@link FsPermission} of this {@link INodeFile}. Since this is a
   * file, the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  @Override
  public void setPermission(FsPermission permission) {
    super.setPermission(permission.applyUMask(UMASK));
  }

  @Override
  public int collectSubtreeBlocksAndClear(List<Block> blocks) {
    collectSubtreeBlocksAndClearNoDelete(blocks);
    EntityManager.remove(this);
    return 1;
  }

  public int collectSubtreeBlocksAndClearNoDelete(List<Block> v) {

    parent = null;
    List<BlockInfo> tempList = new ArrayList<BlockInfo>(getBlocks());
    for (BlockInfo blk : tempList) {
      v.add(blk);
      blk.setINode(null);
      EntityManager.remove(blk);
    }
    blocks.clear();

    return 1;
  }

  /**
   * {@inheritDoc}
   */
  public long[] computeContentSummary(long[] summary) {
    summary[0] += computeFileSize(true);
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  /**
   * Compute file size. May or may not include BlockInfoUnderConstruction.
   */
  public long computeFileSize(boolean includesBlockInfoUnderConstruction) {
    List<BlockInfo> blks = getBlocks();
    if (blks.isEmpty()) {
      return 0;
    }
    final int last = blks.size() - 1;
    //check if the last block is BlockInfoUnderConstruction
    long bytes = blks.get(last) instanceof BlockInfoUnderConstruction
            && !includesBlockInfoUnderConstruction
            ? 0 : blks.get(last).getNumBytes();

    for (int i = 0; i < blks.size() - 1; i++) {
      bytes += blks.get(i).getNumBytes();
    }

    return bytes;
  }

  @Override
  public DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  public long diskspaceConsumed() {
    List<BlockInfo> list = getBlocks();
    Block[] array = list.toArray(new Block[list.size()]);
    return diskspaceConsumed(array);
  }

  public long diskspaceConsumed(Block[] blkArr) {
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
  public BlockInfo getPenultimateBlock() throws IOException {
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

  public String getClientName() {
    return clientName;
  }

  public void setClientName(String clientName) {
    this.clientName = clientName;
  }

  public String getClientMachine() {
    return clientMachine;
  }

  public DatanodeID getClientNode() {
    return clientNode;
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  public boolean isUnderConstruction() {
    return underConstruction;
  }
  
  public void convertToCompleteInode() {
    this.underConstruction = false;
  }
  
  public void convertToUnderConstruction(String clientName, String clientMachine, DatanodeID clientNode) {
    this.underConstruction = true;
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }
  
  /**
   * Convert the last block of the file to an under-construction block. Set its
   * locations.
   */
  public BlockInfoUnderConstruction setLastBlock(BlockInfo lastBlock)
          throws IOException {
    if (getBlocks().isEmpty()) {
      throw new IOException("Trying to update non-existant block. "
              + "File is empty.");
    }

    BlockInfoUnderConstruction bUc;
    if (getBlocks().contains(lastBlock));
    removeBlock(lastBlock);
    if (lastBlock.isComplete()) {
      bUc = new BlockInfoUnderConstruction(lastBlock);
    } else {
      bUc = (BlockInfoUnderConstruction) lastBlock;
      bUc.setBlockUCState(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION);
    }

    bUc.setINode(this);
    addBlock(bUc);
    return bUc;
  }
}
