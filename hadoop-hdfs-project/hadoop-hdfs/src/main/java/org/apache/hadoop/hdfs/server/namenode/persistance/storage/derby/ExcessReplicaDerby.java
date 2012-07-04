package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.ExcessReplicaContext;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaDerby extends ExcessReplicaContext {

  DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  protected TreeSet<ExcessReplica> findExcessReplicaByStorageId(String sId) {
    String query = String.format("select * from %s where %s=?",
            TABLE_NAME, STORAGE_ID);
    try {
      PreparedStatement s = connector.obtainSession().prepareStatement(query);
      s.setString(1, sId);
      ResultSet rSet = s.executeQuery();
      return syncExcessReplicaInstances(rSet);
    } catch (SQLException ex) {
      Logger.getLogger(ExcessReplicaDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return new TreeSet<ExcessReplica>();
  }

  @Override
  protected ExcessReplica findByPkey(Object[] params) {
    String query = String.format("select * from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    long blockId = (Long) params[0];
    String storageId = (String) params[1];
    try {
      PreparedStatement s = connector.obtainSession().prepareStatement(query);
      s.setLong(1, blockId);
      s.setString(2, storageId);
      ResultSet result = s.executeQuery();
      if (result.next()) {
        return createReplica(result);
      }
    } catch (SQLException ex) {
      Logger.getLogger(ExcessReplicaDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  @Override
  public int count(CounterType<ExcessReplica> counter, Object... params) {
    ExcessReplica.Counter eCounter = (ExcessReplica.Counter) counter;
    switch (eCounter) {
      case All:
        String query = String.format("select count(*) from %s", TABLE_NAME);
        try {
          PreparedStatement s = connector.obtainSession().prepareStatement(query);
          ResultSet result = s.executeQuery();
          if (result.next()) {
            return result.getInt(1);
          }
        } catch (SQLException ex) {
          Logger.getLogger(ExcessReplicaDerby.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    return -1;
  }

  @Override
  public void prepare() {
    String insert = String.format("insert into %s values(?,?)", TABLE_NAME);
    String delete = String.format("delete from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (ExcessReplica exReplica : newExReplica.values()) {
        insrt.setLong(1, exReplica.getBlockId());
        insrt.setString(2, exReplica.getStorageId());
        insrt.addBatch();
      }
      insrt.executeBatch();
      PreparedStatement dlt = conn.prepareStatement(delete);
      for (ExcessReplica exReplica : removedExReplica.values()) {
        dlt.setLong(1, exReplica.getBlockId());
        dlt.setString(2, exReplica.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(ExcessReplicaDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private TreeSet<ExcessReplica> syncExcessReplicaInstances(ResultSet rSet) throws SQLException {
    TreeSet<ExcessReplica> replicaSet = new TreeSet<ExcessReplica>();
    while (rSet.next()) {
      ExcessReplica replica = createReplica(rSet);
      if (!removedExReplica.containsKey(replica)) {
        if (exReplicas.containsKey(replica)) {
          replicaSet.add(exReplicas.get(replica));
        } else {
          exReplicas.put(replica, replica);
          replicaSet.add(replica);
        }
      }
    }

    return replicaSet;
  }

  private ExcessReplica createReplica(ResultSet rSet) throws SQLException {
    return new ExcessReplica(rSet.getString(STORAGE_ID), rSet.getLong(BLOCK_ID));
  }

  @Override
  public void removeAll() throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
