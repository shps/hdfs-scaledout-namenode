package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoDerby extends BlockInfoStorage {

  private DerbyConnector connector = DerbyConnector.INSTANCE;
  protected Map<Long, BlockInfo> newBlocks = new HashMap<Long, BlockInfo>();

  @Override
  public void clear() {
    super.clear();
    newBlocks.clear();
  }

  @Override
  public void remove(BlockInfo block) throws TransactionContextException {
    super.remove(block);
    newBlocks.remove(block.getBlockId());
  }

  @Override
  public int countAll() {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select count(*) from %s", TABLE_NAME);
      PreparedStatement s;
      s = conn.prepareStatement(query);

      ResultSet result = s.executeQuery();
      if (result.next()) {
        return result.getInt(1);
      }
    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return -1;
  }

  @Override
  public void add(BlockInfo block) throws TransactionContextException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    newBlocks.put(block.getBlockId(), block);
  }

  @Override
  public void commit() {
    String insertQuery = String.format("insert into %s values(?,?,?,?,?,?,?,?,?)",
            TABLE_NAME);
    String deleteQuery = String.format("delete from %s where %s = ?",
            TABLE_NAME, BLOCK_ID);
    String updateQuery = String.format("update %s set "
            + "%s=?, %s=?, %s=?, %s=?, %s=?, %s=?, %s=?, %s=? where %s=?",
            TABLE_NAME, BLOCK_INDEX, INODE_ID, NUM_BYTES, GENERATION_STAMP,
            BLOCK_UNDER_CONSTRUCTION_STATE, TIME_STAMP, PRIMARY_NODE_INDEX,
            BLOCK_RECOVERY_ID, BLOCK_ID);
    try {
      Connection conn = connector.obtainSession();

      PreparedStatement dlt = conn.prepareStatement(deleteQuery);
      for (BlockInfo block : removedBlocks.values()) {
        dlt.setLong(1, block.getBlockId());
        dlt.addBatch();
      }

      PreparedStatement insrt = conn.prepareStatement(insertQuery);
      for (BlockInfo block : newBlocks.values()) {
        insrt.setLong(1, block.getBlockId());
        insrt.setInt(2, block.getBlockIndex());
        insrt.setLong(3, block.getINode().getID());
        insrt.setLong(4, block.getNumBytes());
        insrt.setLong(5, block.getGenerationStamp());
        insrt.setInt(6, block.getBlockUCState().ordinal());
        insrt.setLong(7, block.getTimestamp());
        if (block instanceof BlockInfoUnderConstruction) {
          BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
          insrt.setInt(8, ucBlock.getPrimaryNodeIndex());
          insrt.setLong(9, ucBlock.getBlockRecoveryId());
        }
        insrt.addBatch();
      }

      PreparedStatement updt = conn.prepareStatement(updateQuery);
      for (BlockInfo block : modifiedBlocks.values()) {
        updt.setLong(9, block.getBlockId());
        updt.setInt(1, block.getBlockIndex());
        updt.setLong(2, block.getINode().getID());
        updt.setLong(3, block.getNumBytes());
        updt.setLong(4, block.getGenerationStamp());
        updt.setInt(5, block.getBlockUCState().ordinal());
        updt.setLong(6, block.getTimestamp());
        if (block instanceof BlockInfoUnderConstruction) {
          BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
          updt.setInt(7, ucBlock.getPrimaryNodeIndex());
          updt.setLong(8, ucBlock.getBlockRecoveryId());
        }
        updt.addBatch();
      }

      dlt.executeBatch();
      insrt.executeBatch();
      updt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  protected List<BlockInfo> findByInodeId(long id) {
    List<BlockInfo> syncedList = null;
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s = ?",
              TABLE_NAME, INODE_ID);
      PreparedStatement s;
      s = conn.prepareStatement(query);
      s.setLong(1, id);
      ResultSet result = s.executeQuery();
      syncedList = syncBlockInfoInstances(result);
    } catch (IOException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return syncedList;
  }

  @Override
  protected List<BlockInfo> findByStorageId(String storageId) {
    List<BlockInfo> syncedList = null;
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s in (select %s from %s where %s=?)",
              TABLE_NAME, BLOCK_ID, IndexedReplicaStorage.BLOCK_ID,
              IndexedReplicaStorage.TABLE_NAME, IndexedReplicaStorage.STORAGE_ID);
      PreparedStatement s;
      s = conn.prepareStatement(query);
      s.setString(1, storageId);
      ResultSet result = s.executeQuery();
      syncedList = syncBlockInfoInstances(result);
    } catch (IOException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return syncedList;
  }

  @Override
  protected List<BlockInfo> findAllBlocks() {
    List<BlockInfo> syncedList = null;
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s", TABLE_NAME);
      PreparedStatement s;
      s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      syncedList = syncBlockInfoInstances(result);
    } catch (IOException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return syncedList;
  }

  @Override
  protected BlockInfo findById(long id) {
    BlockInfo blockInfo = null;
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s=?",
              TABLE_NAME, BLOCK_ID);
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, id);
      ResultSet resultSet = s.executeQuery();
      if (resultSet.next()) {
        blockInfo = createBlockInfo(resultSet);
      }
    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return blockInfo;
  }

  private List<BlockInfo> syncBlockInfoInstances(ResultSet resultSet) throws IOException, SQLException {
    List<BlockInfo> finalList = new ArrayList<BlockInfo>();

    while (resultSet.next()) {
      BlockInfo blockInfo = createBlockInfo(resultSet);
      if (blocks.containsKey(blockInfo.getBlockId()) && !removedBlocks.containsKey(blockInfo.getBlockId())) {
        finalList.add(blocks.get(blockInfo.getBlockId()));
      } else {
        blocks.put(blockInfo.getBlockId(), blockInfo);
        finalList.add(blockInfo);
      }
    }

    return finalList;
  }

  private BlockInfo createBlockInfo(ResultSet rs) {
    BlockInfo blockInfo = null;
    try {
      Block b = new Block(rs.getLong(BLOCK_ID), rs.getLong(NUM_BYTES), rs.getLong(GENERATION_STAMP));
      int ucState = rs.getInt(BLOCK_UNDER_CONSTRUCTION_STATE);
      if (ucState > 0) { //UNDER_CONSTRUCTION, UNDER_RECOVERY, COMMITED
        blockInfo = new BlockInfoUnderConstruction(b);
        ((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.values()[ucState]);
        ((BlockInfoUnderConstruction) blockInfo).setPrimaryNodeIndex(rs.getInt(PRIMARY_NODE_INDEX));
        ((BlockInfoUnderConstruction) blockInfo).setBlockRecoveryId(rs.getLong(BLOCK_RECOVERY_ID));
      } else if (ucState == HdfsServerConstants.BlockUCState.COMPLETE.ordinal()) {
        blockInfo = new BlockInfo(b);
      }

      blockInfo.setINodeId(rs.getLong(INODE_ID));
      blockInfo.setTimestamp(rs.getLong(TIME_STAMP));
      blockInfo.setBlockIndex(rs.getInt(BLOCK_INDEX));

    } catch (SQLException ex) {
      Logger.getLogger(BlockInfoDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return blockInfo;
  }
}
