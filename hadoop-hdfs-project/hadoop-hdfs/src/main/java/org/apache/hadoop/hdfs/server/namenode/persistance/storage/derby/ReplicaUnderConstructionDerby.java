package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaUnderConstruntionDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionDerby extends ReplicaUnderConstruntionDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId) throws StorageException {
    try {
      String query = String.format("select * from %s where %s=?", TABLE_NAME, BLOCK_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, blockId);
      ResultSet rSet = s.executeQuery();
      return createReplicaList(rSet);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public void prepare(Collection<ReplicaUnderConstruction> removed, Collection<ReplicaUnderConstruction> newed, Collection<ReplicaUnderConstruction> modified) throws StorageException {
    try {
      String insert = String.format("insert into %s values(?,?,?,?)", TABLE_NAME);
      String delete = String.format("delete from %s where %s=? and %s=?",
              TABLE_NAME, BLOCK_ID, STORAGE_ID);
      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (ReplicaUnderConstruction r : newed) {
        insrt.setLong(1, r.getBlockId());
        insrt.setString(2, r.getStorageId());
        insrt.setInt(3, r.getState().ordinal());
        insrt.setInt(4, r.getIndex());
        insrt.addBatch();
      }
      insrt.executeBatch();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (ReplicaUnderConstruction r : removed) {
        dlt.setLong(1, r.getBlockId());
        dlt.setString(2, r.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private List<ReplicaUnderConstruction> createReplicaList(ResultSet rSet) throws SQLException {
    List<ReplicaUnderConstruction> replicas = new ArrayList<ReplicaUnderConstruction>();
    while (rSet.next()) {
      replicas.add(new ReplicaUnderConstruction(HdfsServerConstants.ReplicaState.values()[rSet.getInt(STATE)],
              rSet.getString(STORAGE_ID), rSet.getLong(BLOCK_ID), rSet.getInt(REPLICA_INDEX)));
    }
    return replicas;
  }
}
