package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.InvalidatedBlockStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockDerby extends InvalidatedBlockStorage {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  protected List<InvalidatedBlock> findInvalidatedBlockByStorageId(String storageId) {
    String query = String.format("select * from %s where %s=?",
            TABLE_NAME, STORAGE_ID);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, storageId);
      ResultSet rSet = s.executeQuery();
      syncInvalidatedBlockInstances(rSet);

      return new ArrayList<InvalidatedBlock>(invBlocks.values());
    } catch (SQLException ex) {
      Logger.getLogger(InvalidatedBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return new ArrayList<InvalidatedBlock>();
  }

  @Override
  protected List<InvalidatedBlock> findAllInvalidatedBlocks() {
    String query = String.format("select * from %s", TABLE_NAME);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      syncInvalidatedBlockInstances(rSet);

      return new ArrayList<InvalidatedBlock>(invBlocks.values());
    } catch (SQLException ex) {
      Logger.getLogger(InvalidatedBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
    return new ArrayList<InvalidatedBlock>();
  }

  @Override
  protected InvalidatedBlock findInvBlockByPkey(Object[] params) {
    String query = String.format("select * from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    Connection conn = connector.obtainSession();
    InvalidatedBlock result = null;
    try {
      PreparedStatement s = conn.prepareStatement(query);
      long blockId = (Long) params[0];
      String storageId = (String) params[1];
      s.setLong(1, blockId);
      s.setString(2, storageId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        result = new InvalidatedBlock(
                rSet.getString(STORAGE_ID), rSet.getLong(BLOCK_ID),
                rSet.getLong(GENERATION_STAMP), rSet.getLong(NUM_BYTES));
      }
    } catch (SQLException ex) {
      Logger.getLogger(InvalidatedBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return result;
  }

  @Override
  public int countAll() {
    String query = String.format("select count(*) from %s", TABLE_NAME);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return rSet.getInt(1);
      }
    } catch (SQLException ex) {
      Logger.getLogger(InvalidatedBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return -1;
  }

  @Override
  public void commit() {
    String insert = String.format("insert into %s values(?,?,?,?)",
            TABLE_NAME, BLOCK_ID, STORAGE_ID, GENERATION_STAMP, NUM_BYTES);
    String delete = String.format("delete from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (InvalidatedBlock invBlock : newInvBlocks.values()) {
        insrt.setLong(1, invBlock.getBlockId());
        insrt.setString(2, invBlock.getStorageId());
        insrt.setLong(3, invBlock.getGenerationStamp());
        insrt.setLong(4, invBlock.getNumBytes());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (InvalidatedBlock invBlock : removedInvBlocks.values()) {
        dlt.setLong(1, invBlock.getBlockId());
        dlt.setString(2, invBlock.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(InvalidatedBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private void syncInvalidatedBlockInstances(ResultSet rSet) throws SQLException {
    while (rSet.next()) {
      InvalidatedBlock invBlock = new InvalidatedBlock(
              rSet.getString(STORAGE_ID), rSet.getLong(BLOCK_ID),
              rSet.getLong(GENERATION_STAMP), rSet.getLong(NUM_BYTES));
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
        } else {
          invBlocks.put(invBlock, invBlock);
        }
        if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
          storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
        } else {
          HashSet<InvalidatedBlock> invBlockList = new HashSet<InvalidatedBlock>();
          invBlockList.add(invBlock);
          storageIdToInvBlocks.put(invBlock.getStorageId(), invBlockList);
        }
      }
    }
  }
}
