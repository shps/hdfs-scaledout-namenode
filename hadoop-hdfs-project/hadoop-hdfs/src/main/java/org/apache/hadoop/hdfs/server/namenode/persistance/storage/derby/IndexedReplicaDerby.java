package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class IndexedReplicaDerby extends IndexedReplicaStorage {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  protected List<IndexedReplica> findReplicasById(long id) {
    String query = String.format("select * from %s where %s=?", TABLE_NAME, BLOCK_ID);
    Connection conn = connector.obtainSession();
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, id);
      ResultSet rSet = s.executeQuery();
      return createReplicaList(rSet);
    } catch (SQLException ex) {
      Logger.getLogger(IndexedReplicaDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return new ArrayList<IndexedReplica>();
  }

  @Override
  public void commit() {
    String insert = String.format("insert into %s values(?,?)", TABLE_NAME);
    String delete = String.format("delete from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (IndexedReplica replica : newReplicas.values()) {
        insrt.setLong(1, replica.getBlockId());
        insrt.setString(2, replica.getStorageId());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (IndexedReplica replica : removedReplicas.values()) {
        dlt.setLong(1, replica.getBlockId());
        dlt.setString(2, replica.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(ExcessReplicaDerby.class.getName()).log(Level.SEVERE, null, ex);
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
