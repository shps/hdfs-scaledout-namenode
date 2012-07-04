package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

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
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.PendingBlockContext;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockDerby extends PendingBlockContext {

  protected Map<Long, PendingBlockInfo> newPendings = new HashMap<Long, PendingBlockInfo>();
  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  protected List<PendingBlockInfo> findByTimeLimit(long timeLimit) {
    String query = String.format("select * from %s where %s < ?", TABLE_NAME, TIME_STAMP);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, timeLimit);
      ResultSet rSet = s.executeQuery();
      return syncPendingBlockInstances(rSet);
    } catch (SQLException ex) {
      Logger.getLogger(PendingBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  @Override
  protected List<PendingBlockInfo> findAll() {
    String query = String.format("select * from %s", TABLE_NAME);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      syncPendingBlockInstances(rSet);
      return new ArrayList(pendings.values());
    } catch (SQLException ex) {
      Logger.getLogger(PendingBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  @Override
  protected PendingBlockInfo findByPKey(long blockId) {
    String query = String.format("select * from %s where %s=?", TABLE_NAME, BLOCK_ID);
    Connection conn = connector.obtainSession();
    PendingBlockInfo result = null;
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, blockId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        result = new PendingBlockInfo(rSet.getLong(BLOCK_ID),
                rSet.getLong(TIME_STAMP), rSet.getInt(NUM_REPLICAS_IN_PROGRESS));
      }
    } catch (SQLException ex) {
      Logger.getLogger(PendingBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return result;
  }

  @Override
  public void prepare() {
    String insert = String.format("insert into %s values(?,?,?)", TABLE_NAME);
    String update = String.format("update %s set %s=?, %s=? where %s=?",
            TABLE_NAME, TIME_STAMP, NUM_REPLICAS_IN_PROGRESS, BLOCK_ID);
    String delete = String.format("delete from %s where %s=?", TABLE_NAME, BLOCK_ID);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (PendingBlockInfo p : newPendings.values()) {
        insrt.setLong(1, p.getBlockId());
        insrt.setLong(2, p.getTimeStamp());
        insrt.setInt(3, p.getNumReplicas());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement updt = conn.prepareStatement(update);
      for (PendingBlockInfo p : modifiedPendings.values()) {
        updt.setLong(3, p.getBlockId());
        updt.setLong(1, p.getTimeStamp());
        updt.setInt(2, p.getNumReplicas());
        updt.addBatch();
      }
      updt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (PendingBlockInfo p : removedPendings.values()) {
        dlt.setLong(1, p.getBlockId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(PendingBlockDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void add(PendingBlockInfo pendingBlock) throws TransactionContextException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    newPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  @Override
  public void clear() {
    super.clear();
    newPendings.clear();
  }

  @Override
  public void remove(PendingBlockInfo pending) throws TransactionContextException {
    super.remove(pending);
    newPendings.remove(pending.getBlockId());
  }

  private List<PendingBlockInfo> syncPendingBlockInstances(ResultSet rSet) throws SQLException {
    List<PendingBlockInfo> newPBlocks = new ArrayList<PendingBlockInfo>();
    while (rSet.next()) {
      PendingBlockInfo p = new PendingBlockInfo(rSet.getLong(BLOCK_ID),
              rSet.getLong(TIME_STAMP), rSet.getInt(NUM_REPLICAS_IN_PROGRESS));
      if (pendings.containsKey(p.getBlockId())) {
        newPBlocks.add(pendings.get(p.getBlockId()));
      } else if (!removedPendings.containsKey(p.getBlockId())) {
        pendings.put(p.getBlockId(), p);
        newPBlocks.add(p);
      }
    }

    return newPBlocks;
  }

  @Override
  public void removeAll() throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
