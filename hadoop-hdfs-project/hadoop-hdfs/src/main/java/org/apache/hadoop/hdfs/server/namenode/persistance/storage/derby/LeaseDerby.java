package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseDerby extends LeaseDataAccess {

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
    } catch (SQLException ex) {
      handleSQLException(ex);
      return 0;
    }
  }

  @Override
  public Collection<Lease> findByTimeLimit(long timeLimit) throws StorageException {
    try {
      String query = String.format("select * from %s where %s < ?",
              TABLE_NAME, LAST_UPDATE);
      Collection<Lease> results = null;
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, timeLimit);
      return convert(s.executeQuery());
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public Collection<Lease> findAll() throws StorageException {
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
  public Lease findByPKey(String holder) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?",
              TABLE_NAME, HOLDER);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, holder);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return new Lease(rSet.getString(HOLDER),
                rSet.getInt(HOLDER_ID), rSet.getLong(LAST_UPDATE));
      } else {
        return null;
      }
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  @Override
  public Lease findByHolderId(int holderId) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?",
              TABLE_NAME, HOLDER_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setInt(1, holderId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return new Lease(rSet.getString(HOLDER),
                rSet.getInt(HOLDER_ID), rSet.getLong(LAST_UPDATE));
      } else {
        return null;
      }
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  @Override
  public void prepare(Collection<Lease> removed, Collection<Lease> newed, Collection<Lease> modified) throws StorageException {
    String insert = String.format("insert into %s values(?,?,?)", TABLE_NAME);
    String update = String.format("update %s set %s=?, %s=? where %s=?",
            TABLE_NAME, LAST_UPDATE, HOLDER_ID, HOLDER);
    String delete = String.format("delete from %s where %s=?",
            TABLE_NAME, HOLDER);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement updt = conn.prepareStatement(update);
      for (Lease l : modified) {
        updt.setLong(1, l.getLastUpdated());
        updt.setInt(2, l.getHolderID());
        updt.setString(3, l.getHolder());
        updt.addBatch();
      }
      updt.executeBatch();

      PreparedStatement insrt = conn.prepareStatement(insert);
      for (Lease l : newed) {
        insrt.setString(1, l.getHolder());
        insrt.setLong(2, l.getLastUpdated());
        insrt.setInt(3, l.getHolderID());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (Lease l : removed) {
        dlt.setString(1, l.getHolder());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private Collection<Lease> convert(ResultSet rSet) throws SQLException {
    SortedSet<Lease> lSet = new TreeSet<Lease>();
    while (rSet.next()) {
      Lease lease = new Lease(rSet.getString(HOLDER),
              rSet.getInt(HOLDER_ID), rSet.getLong(LAST_UPDATE));
      lSet.add(lease);
    }
    return lSet;
  }
}
