package org.apache.hadoop.hdfs.server.namenode.persistance.storage.mysqlserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author hooman
 */
public class CountHelper {

  public static final String COUNT_QUERY = "select count(*) from %s";
  private static MysqlServerConnector connector = MysqlServerConnector.INSTANCE;

  public static int countAllBlockInfo() throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format(COUNT_QUERY, BlockInfoDataAccess.TABLE_NAME);
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      if (result.next()) {
        return result.getInt(1);
      } else {
        throw new StorageException(String.format("Count result set is empty. Query: %s", query));
      }
    } catch (SQLException ex) {
      throw new StorageException(ex);
    }
  }
}
