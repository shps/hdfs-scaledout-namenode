package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InvalidateBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockDerby extends InvalidateBlockDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public int countAll() throws StorageException {
    try {
      String query = String.format("select count(*) from %s", TABLE_NAME);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return rSet.getInt(1);
      } else {
        return 0;
      }
    } catch (SQLException ex) {
      handleSQLException(ex);
      return 0;
    }
  }

  @Override
  public Collection<InvalidatedBlock> findInvalidatedBlockByStorageId(String storageId) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?",
              TABLE_NAME, STORAGE_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, storageId);
      ResultSet rSet = s.executeQuery();
      return convert(rSet);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }
  
    @Override
  public Collection<InvalidatedBlock> findInvalidatedBlocksByBlockId(long bid) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?",
              TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, bid);
      ResultSet rSet = s.executeQuery();
      return convert(rSet);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public Collection<InvalidatedBlock> findAllInvalidatedBlocks() throws StorageException {
    try {
      String query = String.format("select * from %s", TABLE_NAME);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      return convert(rSet);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public InvalidatedBlock findInvBlockByPkey(Object[] params) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=? and %s=?",
              TABLE_NAME, BLOCK_ID, STORAGE_ID);
      Connection conn = connector.obtainSession();
      InvalidatedBlock result = null;
      PreparedStatement s = conn.prepareStatement(query);
      long blockId = (Long) params[0];
      String storageId = (String) params[1];
      s.setLong(1, blockId);
      s.setString(2, storageId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        result = createInvBlock(rSet);
      }
      return result;
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  private Collection<InvalidatedBlock> findBlocksByPkeys(Collection<InvalidatedBlock> invBlocks) throws StorageException {
    try {
      if (invBlocks.size() > 0) {
        Iterator<InvalidatedBlock> iterator = invBlocks.iterator();
        InvalidatedBlock next = null;
        StringBuilder sIds = new StringBuilder(STORAGE_ID).append(" in (");
        StringBuilder bIds = new StringBuilder(BLOCK_ID).append(" in (");
        String comma = ",";
        String quote = "'";
        while (iterator.hasNext()) {
          next = iterator.next();
          sIds.append(quote).append(next.getStorageId()).append(quote);
          bIds.append(next.getBlockId());
          if (iterator.hasNext()) {
            sIds.append(comma);
            bIds.append(comma);
          }
        }
        sIds.append(")");
        bIds.append(")");
        String query = String.format("select * from %s where %s and %s", TABLE_NAME, sIds.toString(), bIds.toString());
        Connection conn = connector.obtainSession();
        ResultSet rs = conn.createStatement().executeQuery(query);
        return convert(rs);
      } else {
        return Collections.EMPTY_SET;
      }
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public void prepare(Collection<InvalidatedBlock> removed, Collection<InvalidatedBlock> newed, Collection<InvalidatedBlock> modified) throws StorageException {
    try {
      String insert = String.format("insert into %s(%s,%s,%s,%s) values(?,?,?,?)",
              TABLE_NAME, BLOCK_ID, STORAGE_ID, GENERATION_STAMP, NUM_BYTES);
      String update = String.format("update %s set %s=?, %s=? where %s=? and %s=?",
              TABLE_NAME, GENERATION_STAMP, NUM_BYTES, STORAGE_ID, BLOCK_ID);
      String delete = String.format("delete from %s where %s=? and %s=?",
              TABLE_NAME, BLOCK_ID, STORAGE_ID);
      Connection conn = connector.obtainSession();
      Collection<InvalidatedBlock> existings = findBlocksByPkeys(newed);
      PreparedStatement insrt = conn.prepareStatement(insert);
      PreparedStatement updt = conn.prepareStatement(update);
      for (InvalidatedBlock newBlock : newed) {
        if (!existings.contains(newBlock)) {
          insrt.setLong(1, newBlock.getBlockId());
          insrt.setString(2, newBlock.getStorageId());
          insrt.setLong(3, newBlock.getGenerationStamp());
          insrt.setLong(4, newBlock.getNumBytes());
          insrt.addBatch();
        } else {
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
      for (InvalidatedBlock invBlock : removed) {
        dlt.setLong(1, invBlock.getBlockId());
        dlt.setString(2, invBlock.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private HashSet<InvalidatedBlock> convert(ResultSet rSet) throws SQLException {
    HashSet<InvalidatedBlock> result = new HashSet<InvalidatedBlock>();
    while (rSet.next()) {
      result.add(createInvBlock(rSet));
    }
    return result;
  }

  private InvalidatedBlock createInvBlock(ResultSet rSet) throws SQLException {
    return new InvalidatedBlock(
            rSet.getString(STORAGE_ID), rSet.getLong(BLOCK_ID),
            rSet.getLong(GENERATION_STAMP), rSet.getLong(NUM_BYTES));
  }
}
