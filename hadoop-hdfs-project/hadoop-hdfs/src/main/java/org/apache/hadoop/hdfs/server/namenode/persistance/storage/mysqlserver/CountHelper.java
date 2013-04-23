package org.apache.hadoop.hdfs.server.namenode.persistance.storage.mysqlserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 * This class is to do count operations using Mysql Server.
 *
 * @author hooman
 */
public class CountHelper {

  public static final String COUNT_QUERY = "select count(*) from %s";
  private static MysqlServerConnector connector = MysqlServerConnector.INSTANCE;

  /**
   * Counts the number of rows in a given table.
   *
   * This creates and closes connection in every request.
   *
   * @param tableName
   * @return Total number of rows a given table.
   * @throws StorageException
   */
  public static int countAll(String tableName) throws StorageException {
    // TODO[H]: Is it good to create and close connections in every call?
    String query = String.format(COUNT_QUERY, tableName);
    return count(query);
  }

  private static int count(String query) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      if (result.next()) {
        return result.getInt(1);
      } else {
        throw new StorageException(
                String.format("Count result set is empty. Query: %s", query));
      }
    } catch (SQLException ex) {
      throw new StorageException(ex);
    } finally {
      connector.closeSession();
    }
  }

  /**
   * Counts the number of rows in a table specified by the table name where
   * satisfies the given criterion. The criterion should be a valid SLQ
   * statement.
   *
   * @param tableName
   * @param criterion E.g. criterion="id > 100".
   * @return
   */
  public static int countWithCriterion(String tableName, String criterion) throws StorageException {
    StringBuilder queryBuilder = new StringBuilder(String.format(COUNT_QUERY, tableName)).
            append(" where ").
            append(criterion);
    return count(queryBuilder.toString());
  }
}
