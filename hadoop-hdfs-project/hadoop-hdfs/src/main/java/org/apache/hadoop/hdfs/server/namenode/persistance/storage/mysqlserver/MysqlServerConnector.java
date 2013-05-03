package org.apache.hadoop.hdfs.server.namenode.persistance.storage.mysqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 * This class presents a singleton connector to Mysql Server.
 * It creates connections to Mysql Server and loads the driver.
 * 
 * @author hooman
 */
public enum MysqlServerConnector implements StorageConnector<Connection> {

  INSTANCE;
  private Log log;
  private String protocol;
  private String user;
  private String password;
  private ThreadLocal<Connection> connectionPool = new ThreadLocal<Connection>();
  public static final String DEFAULT_PASSWORD = "";
  public static final String DRIVER = "com.mysql.jdbc.Driver";

  private MysqlServerConnector() {
    log = LogFactory.getLog(MysqlServerConnector.class);
  }

  @Override
  public void setConfiguration(Configuration conf) {
    this.protocol = conf.get(DFSConfigKeys.DFS_STORAGE_MYSQL_PROTOCOL_KEY);
    this.user = conf.get(DFSConfigKeys.DFS_STORAGE_MYSQL_USER_KEY);
    this.password = conf.get(DFSConfigKeys.DFS_STORAGE_MYSQL_PASSWORD_KEY, DEFAULT_PASSWORD);
    loadDriver();
  }

  private void loadDriver() {
    try {
      // TODO: [H] throw StorageException, do not catch them here.
      Class.forName(DRIVER).newInstance();
      log.info("Loaded Mysql driver.");
    } catch (ClassNotFoundException cnfe) {
      log.error("\nUnable to load the JDBC driver " + DRIVER, cnfe);
    } catch (InstantiationException ie) {
      log.error("\nUnable to instantiate the JDBC driver " + DRIVER, ie);
    } catch (IllegalAccessException iae) {
      log.error("\nNot allowed to access the JDBC driver " + DRIVER, iae);
    }
  }

  @Override
  public Connection obtainSession() throws StorageException {
    Connection conn = connectionPool.get();
    if (conn == null) {
      try {
        conn = DriverManager.getConnection(protocol, user, password);
        connectionPool.set(conn);
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
    return conn;
  }
  
  public void closeSession() throws StorageException
  {
    Connection conn = connectionPool.get();
    if (conn != null) {
      try {
        conn.close();
        connectionPool.remove();
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
  }

  @Override
  public void beginTransaction() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commit() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void rollback() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean formatStorage() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isTransactionActive() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void stopStorage() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void readLock() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void writeLock() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void readCommitted() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setPartitionKey(Class className, Object key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
