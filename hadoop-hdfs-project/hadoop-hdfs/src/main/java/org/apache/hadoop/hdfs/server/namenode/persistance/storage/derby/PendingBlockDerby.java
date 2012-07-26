package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.PendingBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockDerby extends PendingBlockDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public List<PendingBlockInfo> findByTimeLimit(long timeLimit) throws StorageException {
    try {
      String query = String.format("select * from %s where %s < ?", TABLE_NAME, TIME_STAMP);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, timeLimit);
      return createList(s.executeQuery());
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public List<PendingBlockInfo> findAll() throws StorageException {
    try {
      String query = String.format("select * from %s", TABLE_NAME);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      return createList(s.executeQuery());
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public PendingBlockInfo findByPKey(long blockId) throws StorageException {

    try {
      String query = String.format("select * from %s where %s=?", TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PendingBlockInfo result = null;
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, blockId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        result = new PendingBlockInfo(rSet.getLong(BLOCK_ID),
                rSet.getLong(TIME_STAMP), rSet.getInt(NUM_REPLICAS_IN_PROGRESS));
      }
      return result;
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  @Override
  public void prepare(Collection<PendingBlockInfo> removed, Collection<PendingBlockInfo> newed, Collection<PendingBlockInfo> modified) throws StorageException {
    try {
      String insert = String.format("insert into %s values(?,?,?)", TABLE_NAME);
      String update = String.format("update %s set %s=?, %s=? where %s=?",
              TABLE_NAME, TIME_STAMP, NUM_REPLICAS_IN_PROGRESS, BLOCK_ID);
      String delete = String.format("delete from %s where %s=?", TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (PendingBlockInfo p : newed) {
        insrt.setLong(1, p.getBlockId());
        insrt.setLong(2, p.getTimeStamp());
        insrt.setInt(3, p.getNumReplicas());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement updt = conn.prepareStatement(update);
      for (PendingBlockInfo p : modified) {
        updt.setLong(3, p.getBlockId());
        updt.setLong(1, p.getTimeStamp());
        updt.setInt(2, p.getNumReplicas());
        updt.addBatch();
      }
      updt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (PendingBlockInfo p : removed) {
        dlt.setLong(1, p.getBlockId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private List<PendingBlockInfo> createList(ResultSet rSet) throws SQLException {
    List<PendingBlockInfo> newPBlocks = new ArrayList<PendingBlockInfo>();
    while (rSet.next()) {
      PendingBlockInfo p = new PendingBlockInfo(rSet.getLong(BLOCK_ID),
              rSet.getLong(TIME_STAMP), rSet.getInt(NUM_REPLICAS_IN_PROGRESS));
      newPBlocks.add(p);
    }

    return newPBlocks;
  }
}
