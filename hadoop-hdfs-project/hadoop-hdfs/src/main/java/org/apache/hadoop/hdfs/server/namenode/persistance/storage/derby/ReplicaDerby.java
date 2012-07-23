package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaDerby extends ReplicaDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public List<IndexedReplica> findReplicasById(long id) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?", TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, id);
      ResultSet rSet = s.executeQuery();
      return createReplicaList(rSet);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public void prepare(Collection<IndexedReplica> removed, Collection<IndexedReplica> newed, Collection<IndexedReplica> modified) throws StorageException {
    try {
      String insert = String.format("insert into %s values(?,?,?)", TABLE_NAME);
      String delete = String.format("delete from %s where %s=? and %s=?",
              TABLE_NAME, BLOCK_ID, STORAGE_ID);
      String update = String.format("update %s set %s=? where %s=? and %s=?",
              TABLE_NAME, REPLICA_INDEX, BLOCK_ID, STORAGE_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (IndexedReplica replica : newed) {
        insrt.setLong(1, replica.getBlockId());
        insrt.setString(2, replica.getStorageId());
        insrt.setInt(3, replica.getIndex());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement updt = conn.prepareStatement(update);
      for (IndexedReplica replica : modified) {
        updt.setLong(2, replica.getBlockId());
        updt.setString(3, replica.getStorageId());
        updt.setInt(1, replica.getIndex());
        updt.addBatch();
      }
      updt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (IndexedReplica replica : removed) {
        dlt.setLong(1, replica.getBlockId());
        dlt.setString(2, replica.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private List<IndexedReplica> createReplicaList(ResultSet rSet) throws SQLException {
    List<IndexedReplica> replicas = new ArrayList<IndexedReplica>();
    while (rSet.next()) {
      replicas.add(new IndexedReplica(rSet.getLong(BLOCK_ID),
              rSet.getString(STORAGE_ID), rSet.getInt(REPLICA_INDEX)));
    }
    return replicas;
  }
}
