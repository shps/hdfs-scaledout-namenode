package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    String update = String.format("update %s set %s=?, %s=? where %s=? and %s=?", 
            TABLE_NAME, GENERATION_STAMP, NUM_BYTES, STORAGE_ID, BLOCK_ID);
    String delete = String.format("delete from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    Connection conn = connector.obtainSession();
    try {
      HashSet<InvalidatedBlock> existings = findBlocksByPkeys(newInvBlocks.values());
      PreparedStatement insrt = conn.prepareStatement(insert);
      PreparedStatement updt = conn.prepareStatement(update);
      for (InvalidatedBlock newBlock : newInvBlocks.values()) {
        if (!existings.contains(newBlock))
        {
          insrt.setLong(1, newBlock.getBlockId());
          insrt.setString(2, newBlock.getStorageId());
          insrt.setLong(3, newBlock.getGenerationStamp());
          insrt.setLong(4, newBlock.getNumBytes());
          insrt.addBatch();
        }
        else
        {
          updt.setLong(4, newBlock.getBlockId());
          updt.setString(3, newBlock.getStorageId());
          updt.setLong(1, newBlock.getGenerationStamp());
          updt.setLong(2, newBlock.getNumBytes());
          updt.addBatch();
        }
      }
      insrt.executeBatch();
      updt.executeBatch();

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

  private HashSet<InvalidatedBlock> findBlocksByPkeys(Collection<InvalidatedBlock> invBlocks) {
    if (invBlocks.size() > 0) {
      Iterator<InvalidatedBlock> iterator = invBlocks.iterator();
      InvalidatedBlock next = iterator.next();
      StringBuilder query = new StringBuilder("select * from ").append(TABLE_NAME).
              append(" where (").append(STORAGE_ID).append("='").
              append(next.getStorageId()).append("' and ").append(BLOCK_ID).append("=").
              append(next.getBlockId()).append(")");
      while (iterator.hasNext()) {
        next = iterator.next();
        query.append(" or (").append(STORAGE_ID).append("='").
                append(next.getStorageId()).append("' and ").append(BLOCK_ID).append("=").
                append(next.getBlockId()).append(")");
      }
      Connection conn = connector.obtainSession();
      try {
        ResultSet rs = conn.createStatement().executeQuery(query.toString());
        return syncInvalidatedBlockInstances(rs);
      } catch (SQLException ex) {
        Logger.getLogger(InvalidatedBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

    return new HashSet<InvalidatedBlock>();
  }

  private HashSet<InvalidatedBlock> syncInvalidatedBlockInstances(ResultSet rSet) throws SQLException {
    HashSet<InvalidatedBlock> result = new HashSet<InvalidatedBlock>();
    while (rSet.next()) {
      InvalidatedBlock next = new InvalidatedBlock(
              rSet.getString(STORAGE_ID), rSet.getLong(BLOCK_ID),
              rSet.getLong(GENERATION_STAMP), rSet.getLong(NUM_BYTES));
      if (!removedInvBlocks.containsKey(next)) {
        if (invBlocks.containsKey(next)) {
          result.add(invBlocks.get(next));
        } else {
          invBlocks.put(next, next);
          result.add(next);
        }
        if (storageIdToInvBlocks.containsKey(next.getStorageId())) {
          storageIdToInvBlocks.get(next.getStorageId()).add(next);
        } else {
          HashSet<InvalidatedBlock> invBlockList = new HashSet<InvalidatedBlock>();
          invBlockList.add(next);
          storageIdToInvBlocks.put(next.getStorageId(), invBlockList);
        }
      }
    }

    return result;
  }
}
