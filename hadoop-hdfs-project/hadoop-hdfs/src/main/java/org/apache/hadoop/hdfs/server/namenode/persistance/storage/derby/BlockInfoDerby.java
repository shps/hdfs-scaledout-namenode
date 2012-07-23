package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.hsqldb.Types;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoDerby extends BlockInfoDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public int countAll() throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select count(*) from %s", TABLE_NAME);
      PreparedStatement s;
      s = conn.prepareStatement(query);

      ResultSet result = s.executeQuery();
      return result.getInt(1);
    } catch (SQLException e) {
      handleSQLException(e);
      return 0;
    }
  }

  @Override
  public void prepare(Collection<BlockInfo> removed, Collection<BlockInfo> news, Collection<BlockInfo> modified) throws StorageException {
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
      for (BlockInfo block : removed) {
        dlt.setLong(1, block.getBlockId());
        dlt.addBatch();
      }

      PreparedStatement insrt = conn.prepareStatement(insertQuery);
      for (BlockInfo block : news) {
        insrt.setLong(1, block.getBlockId());
        insrt.setInt(2, block.getBlockIndex());
        insrt.setLong(3, block.getInodeId());
        insrt.setLong(4, block.getNumBytes());
        insrt.setLong(5, block.getGenerationStamp());
        insrt.setInt(6, block.getBlockUCState().ordinal());
        insrt.setLong(7, block.getTimestamp());
        if (block instanceof BlockInfoUnderConstruction) {
          BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
          insrt.setInt(8, ucBlock.getPrimaryNodeIndex());
          insrt.setLong(9, ucBlock.getBlockRecoveryId());
        } else {
          insrt.setNull(8, Types.INTEGER);
          insrt.setNull(9, Types.BIGINT);
        }
        insrt.addBatch();
      }

      PreparedStatement updt = conn.prepareStatement(updateQuery);
      for (BlockInfo block : modified) {
        updt.setLong(9, block.getBlockId());
        updt.setInt(1, block.getBlockIndex());
        updt.setLong(2, block.getInodeId());
        updt.setLong(3, block.getNumBytes());
        updt.setLong(4, block.getGenerationStamp());
        updt.setInt(5, block.getBlockUCState().ordinal());
        updt.setLong(6, block.getTimestamp());
        if (block instanceof BlockInfoUnderConstruction) {
          BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
          updt.setInt(7, ucBlock.getPrimaryNodeIndex());
          updt.setLong(8, ucBlock.getBlockRecoveryId());
        } else {
          updt.setNull(7, Types.INTEGER);
          updt.setNull(8, Types.BIGINT);
        }
        updt.addBatch();
      }

      dlt.executeBatch();
      insrt.executeBatch();
      updt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  @Override
  public List<BlockInfo> findByInodeId(long id) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s = ?",
              TABLE_NAME, INODE_ID);
      PreparedStatement s;
      s = conn.prepareStatement(query);
      s.setLong(1, id);
      ResultSet result = s.executeQuery();
      return createList(result);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public List<BlockInfo> findByStorageId(String storageId) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s in (select %s from %s where %s=?)",
              TABLE_NAME, BLOCK_ID, BLOCK_ID,
              ReplicaDataAccess.TABLE_NAME, ReplicaDataAccess.STORAGE_ID);
      PreparedStatement s;
      s = conn.prepareStatement(query);
      s.setString(1, storageId);
      ResultSet result = s.executeQuery();
      return createList(result);
    } catch (SQLException e) {
      handleSQLException(e);
      return Collections.EMPTY_LIST;
    }

  }

  @Override
  public List<BlockInfo> findAllBlocks() throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s", TABLE_NAME);
      PreparedStatement s;
      s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      return createList(result);
    } catch (SQLException ex) {
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public BlockInfo findById(long id) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s=?",
              TABLE_NAME, BLOCK_ID);
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, id);
      ResultSet resultSet = s.executeQuery();
      if (resultSet.next()) {
        return createBlockInfo(resultSet);
      } else {
        return null;
      }
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  private List<BlockInfo> createList(ResultSet resultSet) throws StorageException {
    List<BlockInfo> finalList = new ArrayList<BlockInfo>();
    try {
      while (resultSet.next()) {
        BlockInfo blockInfo = createBlockInfo(resultSet);
        finalList.add(blockInfo);
      }
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
    return finalList;
  }

  private BlockInfo createBlockInfo(ResultSet rs) throws StorageException {
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
      handleSQLException(ex);
    }

    return blockInfo;
  }
}
