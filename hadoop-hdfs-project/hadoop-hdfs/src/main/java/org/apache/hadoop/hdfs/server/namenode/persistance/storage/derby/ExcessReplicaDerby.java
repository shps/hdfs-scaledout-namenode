package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ExcessReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaDerby implements ExcessReplicaDataAccess {

  DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public int countAll() throws StorageException {
    String query = String.format("select count(*) from %s", TABLE_NAME);
    try {
      PreparedStatement s = connector.obtainSession().prepareStatement(query);
      ResultSet result = s.executeQuery();
      return result.getInt(1);
    } catch (Exception ex) {
      throw new StorageException(ex);
    }
  }

  @Override
  public List<ExcessReplica> findExcessReplicaByStorageId(String sId) throws StorageException {
    String query = String.format("select * from %s where %s=?",
            TABLE_NAME, STORAGE_ID);
    try {
      PreparedStatement s = connector.obtainSession().prepareStatement(query);
      s.setString(1, sId);
      ResultSet rSet = s.executeQuery();
      return createList(rSet);
    } catch (Exception ex) {
      throw new StorageException(ex);
    }
  }

  @Override
  public ExcessReplica findByPkey(Object[] params) throws StorageException {
    String query = String.format("select * from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    long blockId = (Long) params[0];
    String storageId = (String) params[1];
    try {
      PreparedStatement s = connector.obtainSession().prepareStatement(query);
      s.setLong(1, blockId);
      s.setString(2, storageId);
      ResultSet result = s.executeQuery();
      return createReplica(result);
    } catch (Exception ex) {
      throw new StorageException(ex);
    }
  }

  @Override
  public void prepare(Collection<ExcessReplica> removed, Collection<ExcessReplica> newed, Collection<ExcessReplica> modified) throws StorageException {
    String insert = String.format("insert into %s values(?,?)", TABLE_NAME);
    String delete = String.format("delete from %s where %s=? and %s=?",
            TABLE_NAME, BLOCK_ID, STORAGE_ID);
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (ExcessReplica exReplica : newed) {
        insrt.setLong(1, exReplica.getBlockId());
        insrt.setString(2, exReplica.getStorageId());
        insrt.addBatch();
      }
      insrt.executeBatch();
      PreparedStatement dlt = conn.prepareStatement(delete);
      for (ExcessReplica exReplica : removed) {
        dlt.setLong(1, exReplica.getBlockId());
        dlt.setString(2, exReplica.getStorageId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (Exception ex) {
      throw new StorageException(ex);
    }
  }

  private List<ExcessReplica> createList(ResultSet rSet) throws SQLException {
    List<ExcessReplica> list = new ArrayList<ExcessReplica>();
    while (rSet.next()) {
      list.add(createReplica(rSet));
    }

    return list;
  }

  private ExcessReplica createReplica(ResultSet rSet) throws SQLException {
    return new ExcessReplica(rSet.getString(STORAGE_ID), rSet.getLong(BLOCK_ID));
  }
}
