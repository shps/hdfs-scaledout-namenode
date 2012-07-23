package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeasePathDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathDerby extends LeasePathDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public Collection<LeasePath> findByHolderId(int holderId) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?", TABLE_NAME, HOLDER_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, holderId);
      return createList(s.executeQuery());
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public Collection<LeasePath> findByPrefix(String prefix) throws StorageException {
    try {
      String query = String.format("select * from %s where %s like ?", TABLE_NAME, PATH);
      Connection conn = connector.obtainSession();
      TreeSet<LeasePath> lpSet = null;
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, prefix + "%");
      return createList(s.executeQuery());
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public Collection<LeasePath> findAll() throws StorageException {
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
  public LeasePath findByPKey(String path) throws StorageException {
    String query = String.format("select * from %s where %s=?", TABLE_NAME, PATH);
    LeasePath result = null;
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, path);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return new LeasePath(rSet.getString(PATH), rSet.getInt(HOLDER_ID));
      } else {
        return null;
      }
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  @Override
  public void prepare(Collection<LeasePath> removed, Collection<LeasePath> newed, Collection<LeasePath> modified) throws StorageException {
    try {
      String insert = String.format("insert into %s(%s,%s) values(?,?)", TABLE_NAME,
              HOLDER_ID, PATH);
      String delete = String.format("delete from %s where %s=?", TABLE_NAME, PATH);
      String update = String.format("update %s set %s=? where %s=?", TABLE_NAME,
              HOLDER_ID, PATH);

      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (LeasePath l : newed) {
        insrt.setLong(1, l.getHolderId());
        insrt.setString(2, l.getPath());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement updt = conn.prepareStatement(update);
      for (LeasePath l : modified) {
        updt.setLong(1, l.getHolderId());
        updt.setString(2, l.getPath());
        updt.addBatch();
      }
      updt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (LeasePath l : removed) {
        dlt.setString(1, l.getPath());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private Collection<LeasePath> createList(ResultSet rSet) throws SQLException {
    TreeSet<LeasePath> finalList = new TreeSet<LeasePath>();

    while (rSet.next()) {
      LeasePath lPath = new LeasePath(rSet.getString(PATH), rSet.getInt(HOLDER_ID));
      finalList.add(lPath);
    }

    return finalList;
  }
}
