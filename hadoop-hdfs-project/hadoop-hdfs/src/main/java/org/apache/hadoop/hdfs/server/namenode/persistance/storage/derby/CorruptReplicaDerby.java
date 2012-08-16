package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.CorruptReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class CorruptReplicaDerby extends CorruptReplicaDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public int countAll() throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select count(*) from %s", TABLE_NAME);
      PreparedStatement s;
      s = conn.prepareStatement(query);

      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return rSet.getInt(1);
      } else {
        return 0;
      }
    } catch (SQLException e) {
      handleSQLException(e);
      return 0;
    }
  }

  @Override
  public CorruptReplica findByPk(long blockId, String storageId) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=? and %s=?", TABLE_NAME, BLOCK_ID, STORAGE_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, blockId);
      s.setString(2, storageId);
      ResultSet rSet = s.executeQuery();
      CorruptReplica result = null;
      if (rSet.next()) {
        result = createReplica(rSet);
      }
      return result;
    } catch (SQLException e) {
      handleSQLException(e);
      return null;
    }
  }

  @Override
  public List<CorruptReplica> findAll() throws StorageException {
    try {
      String query = String.format("select * from %s", TABLE_NAME);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet rSet = s.executeQuery();
      return createCorruptReplicaList(rSet);
    } catch (SQLException e) {
      handleSQLException(e);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public List<CorruptReplica> findByBlockId(long blockId) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?", TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, blockId);
      ResultSet rSet = s.executeQuery();
      return createCorruptReplicaList(rSet);
    } catch (SQLException e) {
      handleSQLException(e);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public void prepare(Collection<CorruptReplica> removed, Collection<CorruptReplica> newed,
          Collection<CorruptReplica> modified) throws StorageException {
    try {
      String insert = String.format("insert into %s(%s,%s) values(?,?)",
              TABLE_NAME, BLOCK_ID, STORAGE_ID);
      String delete = String.format("delete from %s where %s=? and %s=?",
              TABLE_NAME, BLOCK_ID, STORAGE_ID);

      Connection conn = connector.obtainSession();
      Collection<CorruptReplica> existings = findBlocksByPkeys(newed);
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (CorruptReplica r : newed) {
        if (!existings.contains(r)) {
          insrt.setLong(1, r.getBlockId());
          insrt.setString(2, r.getStorageId());
          insrt.addBatch();
        }
      }
      insrt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (CorruptReplica r : removed) {
        dlt.setLong(1, r.getBlockId());
        dlt.setString(2, r.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException e) {
      handleSQLException(e);
    }
  }

  private CorruptReplica createReplica(ResultSet rSet) throws SQLException {
    return new CorruptReplica(rSet.getLong(BLOCK_ID), rSet.getString(STORAGE_ID));
  }

  private List<CorruptReplica> createCorruptReplicaList(ResultSet rSet) throws SQLException {
    List<CorruptReplica> replicas = new ArrayList<CorruptReplica>();
    while (rSet.next()) {
      replicas.add(new CorruptReplica(rSet.getLong(BLOCK_ID), rSet.getString(STORAGE_ID)));
    }

    return replicas;
  }

  private Collection<CorruptReplica> findBlocksByPkeys(Collection<CorruptReplica> newed) throws StorageException {
    try {
      if (newed.size() > 0) {
        Iterator<CorruptReplica> iterator = newed.iterator();
        CorruptReplica next = null;
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

  private Collection<CorruptReplica> convert(ResultSet rs) throws SQLException {
    HashSet<CorruptReplica> result = new HashSet<CorruptReplica>();
    while (rs.next()) {
      result.add(createReplica(rs));
    }
    return result;
  }
}
