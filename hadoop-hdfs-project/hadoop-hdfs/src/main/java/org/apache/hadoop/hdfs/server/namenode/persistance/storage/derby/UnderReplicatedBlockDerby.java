package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.UnderReplicatedBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class UnderReplicatedBlockDerby extends UnderReplicatedBlockDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public UnderReplicatedBlock findByBlockId(long blockId) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?",
              TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, blockId);
      ResultSet rSet = s.executeQuery();
      UnderReplicatedBlock result = null;
      if (rSet.next()) {
        result = createBlock(rSet);
      }
      return result;
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  @Override
  public List<UnderReplicatedBlock> findAllSortedByLevel() throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s", TABLE_NAME);
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      List<UnderReplicatedBlock> result = createBlocks(rSet);
      Collections.sort(result, UnderReplicatedBlock.Order.ByLevel);
      return result;
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public List<UnderReplicatedBlock> findByLevel(int level) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?", TABLE_NAME, LEVEL);
      return execQueryByLevel(query, level);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public List<UnderReplicatedBlock> findAllLessThanLevel(int level) throws StorageException {
    try {
      String query = String.format("select * from %s where %s < ?", TABLE_NAME, LEVEL);
      return execQueryByLevel(query, level);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  private List<UnderReplicatedBlock> execQueryByLevel(String query, int level) throws SQLException, StorageException {
    Connection conn = connector.obtainSession();
    PreparedStatement s = conn.prepareStatement(query);
    s.setInt(1, level);
    ResultSet rSet = s.executeQuery();
    return createBlocks(rSet);
  }

  @Override
  public void prepare(Collection<UnderReplicatedBlock> removed, Collection<UnderReplicatedBlock> newed,
          Collection<UnderReplicatedBlock> modified) throws StorageException {
    try {
      String insert = String.format("insert into %s(%s,%s) values(?,?)",
              TABLE_NAME, BLOCK_ID, LEVEL);
      String update = String.format("update %s set %s=? where %s=?",
              TABLE_NAME, LEVEL, BLOCK_ID);
      String delete = String.format("delete from %s where %s=?",
              TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (UnderReplicatedBlock b : newed) {
        insrt.setLong(1, b.getBlockId());
        insrt.setInt(2, b.getLevel());
        insrt.addBatch();
      }
      insrt.executeBatch();
      PreparedStatement updt = conn.prepareStatement(update);
      for (UnderReplicatedBlock b : modified) {
        updt.setInt(1, b.getLevel());
        updt.setLong(2, b.getBlockId());
        updt.addBatch();
      }
      updt.executeBatch();
      PreparedStatement dlt = conn.prepareStatement(delete);
      for (UnderReplicatedBlock b : removed) {
        dlt.setLong(1, b.getBlockId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  @Override
  public void removeAll() throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String deleteAll = String.format("delete from %s", TABLE_NAME);
      PreparedStatement s = conn.prepareStatement(deleteAll);
      s.executeUpdate();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private UnderReplicatedBlock createBlock(ResultSet rSet) throws SQLException {
    return new UnderReplicatedBlock(rSet.getInt(LEVEL), rSet.getLong(BLOCK_ID));
  }

  private List<UnderReplicatedBlock> createBlocks(ResultSet rSet) throws SQLException {
    List<UnderReplicatedBlock> blocks = new ArrayList<UnderReplicatedBlock>();
    while (rSet.next()) {
      blocks.add(createBlock(rSet));
    }

    return blocks;
  }
}
