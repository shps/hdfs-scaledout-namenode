package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.LeaseContext;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseDerby extends LeaseContext {

  protected Map<Lease, Lease> newLeases = new HashMap<Lease, Lease>();
  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public void add(Lease lease) throws TransactionContextException {
    if (removedLeases.containsKey(lease)) {
      throw new TransactionContextException("Removed lease passed to be persisted");
    }

    newLeases.put(lease, lease);
    leases.put(lease.getHolder(), lease);
    idToLease.put(lease.getHolderID(), lease);
  }

  @Override
  protected Collection<Lease> findByTimeLimit(long timeLimit) {
    String query = String.format("select * from %s where %s < ?",
            TABLE_NAME, LAST_UPDATE);
    Collection<Lease> results = null;
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, timeLimit);
      ResultSet rSet = s.executeQuery();
      results = syncLeaseInstances(rSet);
    } catch (SQLException ex) {
      Logger.getLogger(LeaseDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return results;
  }

  @Override
  protected Collection<Lease> findAll() {
    String query = String.format("select * from %s", TABLE_NAME);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      return syncLeaseInstances(rSet);
    } catch (SQLException ex) {
      Logger.getLogger(LeaseDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  @Override
  protected Lease findByPKey(String holder) {
    String query = String.format("select * from %s where %s=?",
            TABLE_NAME, HOLDER);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, holder);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return new Lease(rSet.getString(HOLDER),
                rSet.getInt(HOLDER_ID), rSet.getLong(LAST_UPDATE));
      }
    } catch (SQLException ex) {
      Logger.getLogger(LeaseDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  @Override
  protected Lease findByHolderId(int holderId) {
    String query = String.format("select * from %s where %s=?",
            TABLE_NAME, HOLDER_ID);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setInt(1, holderId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return new Lease(rSet.getString(HOLDER),
                rSet.getInt(HOLDER_ID), rSet.getLong(LAST_UPDATE));
      }
    } catch (SQLException ex) {
      Logger.getLogger(LeaseDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  @Override
  public int count(CounterType<Lease> counter, Object... params) {
    Lease.Counter lCounter = (Lease.Counter) counter;
    switch (lCounter) {
      case All:
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
    }
    return -1;
  }

  @Override
  public void prepare() {
    String insert = String.format("insert into %s values(?,?,?)", TABLE_NAME);
    String update = String.format("update %s set %s=?, %s=? where %s=?",
            TABLE_NAME, LAST_UPDATE, HOLDER_ID, HOLDER);
    String delete = String.format("delete from %s where %s=?",
            TABLE_NAME, HOLDER);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement updt = conn.prepareStatement(update);
      for (Lease l : modifiedLeases.values()) {
        updt.setLong(1, l.getLastUpdated());
        updt.setInt(2, l.getHolderID());
        updt.setString(3, l.getHolder());
        updt.addBatch();
      }
      updt.executeBatch();

      PreparedStatement insrt = conn.prepareStatement(insert);
      for (Lease l : newLeases.values()) {
        insrt.setString(1, l.getHolder());
        insrt.setLong(2, l.getLastUpdated());
        insrt.setInt(3, l.getHolderID());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (Lease l : removedLeases.values()) {
        dlt.setString(1, l.getHolder());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(LeaseDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void clear() {
    super.clear();
    newLeases.clear();
  }

  @Override
  public void remove(Lease lease) throws TransactionContextException {
    super.remove(lease);
    newLeases.remove(lease);
  }

  private Collection<Lease> syncLeaseInstances(ResultSet rSet) throws SQLException {
    SortedSet<Lease> lSet = new TreeSet<Lease>();
    while (rSet.next()) {
      Lease lease = new Lease(rSet.getString(HOLDER),
              rSet.getInt(HOLDER_ID), rSet.getLong(LAST_UPDATE));
      if (!removedLeases.containsKey(lease)) {
        if (leases.containsKey(lease.getHolder())) {
          lSet.add(leases.get(lease.getHolder()));
        } else {
          lSet.add(lease);
          leases.put(lease.getHolder(), lease);
          idToLease.put(lease.getHolderID(), lease);
        }
      }
    }

    return lSet;
  }

  @Override
  public void removeAll() throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
