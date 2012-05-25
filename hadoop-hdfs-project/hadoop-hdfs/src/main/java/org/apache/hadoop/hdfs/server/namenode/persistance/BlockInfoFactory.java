package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import se.sics.clusterj.BlockInfoTable;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class BlockInfoFactory {

  public static void createPersistable(BlockInfo block, BlockInfoTable persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setNumBytes(block.getNumBytes());
    persistable.setGenerationStamp(block.getGenerationStamp());
    persistable.setINodeID(block.getINode().getID());
    persistable.setTimestamp(block.getTimestamp());
    persistable.setBlockIndex(block.getBlockIndex());
    persistable.setBlockUCState(block.getBlockUCState().ordinal());
    if (block instanceof BlockInfoUnderConstruction) {
      BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
      persistable.setPrimaryNodeIndex(ucBlock.getPrimaryNodeIndex());
      persistable.setBlockRecoveryId(ucBlock.getBlockRecoveryId());
    }

  }

  public static BlockInfo createBlockInfo(BlockInfoTable bit) throws IOException {
    Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());
    BlockInfo blockInfo = null;

    if (bit.getBlockUCState() > 0) { //UNDER_CONSTRUCTION, UNDER_RECOVERY, COMMITED
      blockInfo = new BlockInfoUnderConstruction(b);
      ((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.values()[bit.getBlockUCState()]);
      ((BlockInfoUnderConstruction) blockInfo).setPrimaryNodeIndex(bit.getPrimaryNodeIndex());
      ((BlockInfoUnderConstruction) blockInfo).setBlockRecoveryId(bit.getBlockRecoveryId());
    } else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE.ordinal()) {
      blockInfo = new BlockInfo(b);
    }

    blockInfo.setTimestamp(bit.getTimestamp());
    blockInfo.setBlockIndex(bit.getBlockIndex());

    return blockInfo;
  }

  public static List<BlockInfo> createBlockInfoList(List<BlockInfoTable> bitList) throws IOException {
    List<BlockInfo> blocks = new ArrayList<BlockInfo>();
    for (BlockInfoTable bit : bitList) {
      blocks.add(createBlockInfo(bit));
    }
    return blocks;
  }
}
