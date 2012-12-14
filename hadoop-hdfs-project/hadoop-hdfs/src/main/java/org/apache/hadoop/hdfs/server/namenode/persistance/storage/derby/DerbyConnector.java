package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum DerbyConnector implements StorageConnector<Connection> {

  INSTANCE;
  private Log log;
  /*
   * the default framework is embedded
   */
  public static final String DERBY_NETWORK_SERVER = "derby-net";
  public static final String DERBY_EMBEDDED = "derby-em";
  public static final String DEFAULT_DERBY_EMBEDDED_PROTOCOL = "jdbc:derby:memory:derbyDB";
  public static final String DEFAULT_DERBY_NETWORK_SERVER_PROTOCOL = "jdbc:derby://localhost:1527/memory:derbyDB";
  private String framework = null;
  private String driver = null;
  private String protocol = null;
  private ThreadLocal<Connection> connectionPool = new ThreadLocal<Connection>();
  private ThreadLocal<Boolean> activeTransactions = new ThreadLocal<Boolean>();
  private boolean dbStarted = false;

  private DerbyConnector() {
    log = LogFactory.getLog(DerbyConnector.class);
  }

  @Override
  public synchronized void setConfiguration(Configuration conf) {
    framework = conf.get(DFSConfigKeys.DFS_STORAGE_TYPE_KEY);
    if (framework.equals(DERBY_NETWORK_SERVER)) {
      driver = "org.apache.derby.jdbc.ClientDriver";
    } else if (framework.equals(DERBY_EMBEDDED)) {
      this.driver = "org.apache.derby.jdbc.EmbeddedDriver";
    } else {
      throw new RuntimeException("Invalid Storage Type " + framework);
    }

    this.protocol = conf.get(DFSConfigKeys.DFS_STORAGE_DERBY_PROTOCOL_KEY);

    if (!dbStarted) {
      loadDriver();
      try {
        startDatabase();
      } catch (StorageException ex) {
        throw new RuntimeException(ex);
      }
    }

  }

  @Override
  public Connection obtainSession() throws StorageException {
    Connection conn = this.connectionPool.get();
    if (conn == null) {
      try {
        conn = DriverManager.getConnection(protocol + ";create=false");
        conn.setAutoCommit(false);
        this.connectionPool.set(conn);
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }

    return conn;
  }

  @Override
  public void beginTransaction() throws StorageException {
    Connection conn = obtainSession(); //reserve a connection for this thread.
    activeTransactions.set(true);
  }

  @Override
  public void commit() throws StorageException {
    Connection connection = connectionPool.get();
    if (!isTransactionActive()) {
      throw new StorageException("The transaction is not began!");
    }

    try {
      connection.commit();
    } catch (SQLException ex) {
      throw new StorageException(ex);
    } finally {
      this.activeTransactions.set(false);
    }
  }

  @Override
  public void rollback() throws StorageException {

    if (isTransactionActive()) {
      Connection connection = connectionPool.get();
      try {
        connection.rollback();
      } catch (SQLException ex) {
        throw new StorageException(ex);
      } finally {
        this.activeTransactions.set(false);
      }
    }
  }

  @Override
  public boolean formatStorage() throws StorageException {
    Connection conn = null;
    Statement s = null;
    try {
      conn = DriverManager.getConnection(protocol + ";create=false");

      conn.setAutoCommit(false);

      s = conn.createStatement();
      s.execute(String.format("delete from %s", BlockInfoDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", CorruptReplicaDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", InodeDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", LeaseDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", LeasePathDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", PendingBlockDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", ReplicaDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", InvalidateBlockDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", ExcessReplicaDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", ReplicaUnderConstruntionDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", UnderReplicatedBlockDataAccess.TABLE_NAME));
      s.execute(String.format("delete from %s", LeaderDataAccess.TABLE_NAME));

      //commit changes
      conn.commit();
      log.info("formatted database with protocol: " + protocol);
      return true;
    } catch (SQLException ex) {
      throw new StorageException(ex);
    } finally {
      try {
        if (s != null && !s.isClosed()) {
          s.close();
          s = null;
        }
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
          conn = null;
        }
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
  }

  @Override
  public boolean isTransactionActive() {
    return this.activeTransactions.get();
  }

  private void startDatabase() throws StorageException {
    log.info("Database is starting in " + framework + " mode");

    /*
     * load the desired JDBC driver
     */

    Connection conn = null;
    Statement s = null;
    try {
      /*
       * This connection specifies create=true in the connection URL to cause
       * the database to be created when connecting for the first time. To
       * remove the database, remove the directory derbyDB (the same as the
       * database name) and its contents.
       *
       * The directory derbyDB will be created under the directory that the
       * system property derby.system.home points to, or the current directory
       * (user.dir) if derby.system.home is not set.
       */
      conn = DriverManager.getConnection(protocol + ";create=true");
      ResultSet tables = conn.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
      if (tables.next()) {
        log.info("No need to create tables, The following tables already exists:");
        do {
          log.info(tables.getString("TABLE_NAME"));
        } while (tables.next());
        dbStarted = true;
        return;
      }

      log.info("Connected to and created database " + protocol);

      // We want to control transactions manually. Autocommit is on by
      // default in JDBC.
      conn.setAutoCommit(false);

      s = conn.createStatement();
      createTables(s);

      //commit changes
      conn.commit();
      dbStarted = true;
    } catch (SQLException ex) {
      dbStarted = false;
      throw new StorageException(ex);
    } finally {
      try {
        if (s != null && !s.isClosed()) {
          s.close();
          s = null;
        }
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
          conn = null;
        }
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
  }

  @Override
  public synchronized void stopStorage() {
    if (this.dbStarted) {
      if (framework.equals(DERBY_EMBEDDED)) {
        try {
          connectionPool = new ThreadLocal<Connection>();
          activeTransactions = new ThreadLocal<Boolean>();
          // the shutdown=true attribute shuts down Derby
          DriverManager.getConnection(protocol + ";drop=true");
          // To shut down a specific database only, but keep the
          // engine running (for example for connecting to other
          // databases), specify a database in the connection URL:
          //DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true");
        } catch (SQLException se) {
          if (((se.getErrorCode() == 45000 || se.getErrorCode() == -1)
                  && ("08006".equals(se.getSQLState())))) {
            this.dbStarted = false;
            // we got the expected exception
            log.info("Derby shut down normally");
            // Note that for single database shutdown, the expected
            // SQL state is "08006", and the error code is 45000.
          } else {
            // if the error code or SQLState is different, we have
            // an unexpected exception (shutdown failed)
            log.error("Derby did not shut down normally");
          }
        }
      }
    }
  }

  /*
   * Loads the appropriate JDBC driver for this environment/framework. For
   * example, if we are in an embedded environment, we load Derby's embedded
   * Driver, <code>org.apache.derby.jdbc.EmbeddedDriver</code>.
   */
  private void loadDriver() {
    /*
     * The JDBC driver is loaded by loading its class. If you are using JDBC 4.0
     * (Java SE 6) or newer, JDBC drivers may be automatically loaded, making
     * this code optional.
     *
     * In an embedded environment, this will also start up the Derby engine
     * (though not any databases), since it is not already running. In a client
     * environment, the Derby engine is being run by the network server
     * framework.
     *
     * In an embedded environment, any static Derby system properties must be
     * set before loading the driver to take effect.
     */
    try {
//      String driver2 = "org.apache.derby.jdbc.ClientDriver";
      Class.forName(driver).newInstance();
      log.info("Loaded the appropriate driver");
    } catch (ClassNotFoundException cnfe) {
      log.error("\nUnable to load the JDBC driver " + driver, cnfe);
    } catch (InstantiationException ie) {
      log.error("\nUnable to instantiate the JDBC driver " + driver, ie);
    } catch (IllegalAccessException iae) {
      log.error("\nNot allowed to access the JDBC driver " + driver, iae);
    }
  }

  private void createTables(Statement s) throws SQLException {
    log.info("Creating tables...");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,"
            + "%s INTEGER DEFAULT NULL,"
            + "%s BIGINT DEFAULT NULL,"
            + "%s BIGINT DEFAULT NULL,"
            + "%s BIGINT DEFAULT NULL,"
            + "%s INTEGER DEFAULT NULL,"
            + "%s BIGINT DEFAULT NULL,"
            + "%s INTEGER DEFAULT NULL,"
            + "%s BIGINT DEFAULT NULL,"
            + "PRIMARY KEY (%s))", BlockInfoDataAccess.TABLE_NAME,
            BlockInfoDataAccess.BLOCK_ID, BlockInfoDataAccess.BLOCK_INDEX,
            BlockInfoDataAccess.INODE_ID, BlockInfoDataAccess.NUM_BYTES,
            BlockInfoDataAccess.GENERATION_STAMP, BlockInfoDataAccess.BLOCK_UNDER_CONSTRUCTION_STATE,
            BlockInfoDataAccess.TIME_STAMP, BlockInfoDataAccess.PRIMARY_NODE_INDEX,
            BlockInfoDataAccess.BLOCK_RECOVERY_ID, BlockInfoDataAccess.BLOCK_ID));
    log.info("Table block_info is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,"
            + "%s VARCHAR(128) NOT NULL,"
            + "PRIMARY KEY (%s,%s))", CorruptReplicaDataAccess.TABLE_NAME,
            CorruptReplicaDataAccess.BLOCK_ID, CorruptReplicaDataAccess.STORAGE_ID,
            CorruptReplicaDataAccess.BLOCK_ID, CorruptReplicaDataAccess.STORAGE_ID));
    log.info("Table CorruptReplica is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,"
            + "%s VARCHAR(128) NOT NULL,"
            + "PRIMARY KEY (%s,%s))", ExcessReplicaDataAccess.TABLE_NAME,
            ExcessReplicaDataAccess.BLOCK_ID, ExcessReplicaDataAccess.STORAGE_ID,
            ExcessReplicaDataAccess.BLOCK_ID, ExcessReplicaDataAccess.STORAGE_ID));
    log.info("Table ExcessReplica is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s VARCHAR (128) FOR BIT DATA DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "%s varchar(45) DEFAULT NULL,   "
            + "%s varchar(45) DEFAULT NULL,   "
            + "%s varchar(45) DEFAULT NULL,   "
            + "%s CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s varchar(8000) DEFAULT NULL,  "
            + "PRIMARY KEY (%s) )", InodeDataAccess.TABLE_NAME,
            InodeDataAccess.ID, InodeDataAccess.NAME, InodeDataAccess.PARENT_ID,
            InodeDataAccess.IS_DIR, InodeDataAccess.MODIFICATION_TIME,
            InodeDataAccess.ACCESS_TIME, InodeDataAccess.PERMISSION, InodeDataAccess.NSQUOTA,
            InodeDataAccess.DSQUOTA, InodeDataAccess.IS_UNDER_CONSTRUCTION,
            InodeDataAccess.CLIENT_NAME, InodeDataAccess.CLIENT_MACHINE,
            InodeDataAccess.CLIENT_NODE, InodeDataAccess.IS_CLOSED_FILE,
            InodeDataAccess.HEADER, InodeDataAccess.IS_DIR_WITH_QUOTA,
            InodeDataAccess.NSCOUNT, InodeDataAccess.DSCOUNT, InodeDataAccess.SYMLINK,
            InodeDataAccess.ID));
    log.info(String.format("Table %s is created.", InodeDataAccess.TABLE_NAME));

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "PRIMARY KEY (%s,%s) )", InvalidateBlockDataAccess.TABLE_NAME,
            InvalidateBlockDataAccess.BLOCK_ID, InvalidateBlockDataAccess.STORAGE_ID,
            InvalidateBlockDataAccess.GENERATION_STAMP, InvalidateBlockDataAccess.NUM_BYTES,
            InvalidateBlockDataAccess.BLOCK_ID, InvalidateBlockDataAccess.STORAGE_ID));
    log.info(String.format("Table %s is created.", InvalidateBlockDataAccess.TABLE_NAME));

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s varchar(255) NOT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s INTEGER DEFAULT NULL,   "
            + "PRIMARY KEY (%s) )", LeaseDataAccess.TABLE_NAME,
            LeaseDataAccess.HOLDER, LeaseDataAccess.LAST_UPDATE, LeaseDataAccess.HOLDER_ID,
            LeaseDataAccess.HOLDER));
    log.info(String.format("Table %s is created.", LeaseDataAccess.TABLE_NAME));

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s INTEGER NOT NULL,   "
            + "%s varchar(255) NOT NULL,   "
            + "PRIMARY KEY (%s) )", LeasePathDataAccess.TABLE_NAME,
            LeasePathDataAccess.HOLDER_ID, LeasePathDataAccess.PATH, LeasePathDataAccess.PATH));
    log.info(String.format("Table %s is created.", LeasePathDataAccess.TABLE_NAME));

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s BIGINT NOT NULL, %s BIGINT NOT NULL,"
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s) )", PendingBlockDataAccess.TABLE_NAME,
            PendingBlockDataAccess.BLOCK_ID, PendingBlockDataAccess.TIME_STAMP,
            PendingBlockDataAccess.NUM_REPLICAS_IN_PROGRESS, PendingBlockDataAccess.BLOCK_ID));
    log.info(String.format("Table %s is created.", PendingBlockDataAccess.TABLE_NAME));

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s INTEGER DEFAULT NULL,   "
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s,%s) )", ReplicaUnderConstruntionDataAccess.TABLE_NAME,
            ReplicaUnderConstruntionDataAccess.BLOCK_ID, ReplicaUnderConstruntionDataAccess.STORAGE_ID,
            ReplicaUnderConstruntionDataAccess.STATE, ReplicaUnderConstruntionDataAccess.REPLICA_INDEX,
            ReplicaUnderConstruntionDataAccess.BLOCK_ID, ReplicaUnderConstruntionDataAccess.STORAGE_ID));
    log.info(String.format("Table %s is created.", ReplicaUnderConstruntionDataAccess.TABLE_NAME));

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s,%s) )", ReplicaDataAccess.TABLE_NAME,
            ReplicaDataAccess.BLOCK_ID, ReplicaDataAccess.STORAGE_ID,
            ReplicaDataAccess.REPLICA_INDEX, ReplicaDataAccess.BLOCK_ID,
            ReplicaDataAccess.STORAGE_ID));
    log.info(String.format("Table %s is created.", ReplicaDataAccess.TABLE_NAME));

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,"
            + "%s INTEGER DEFAULT NULL,"
            + "PRIMARY KEY (%s))", UnderReplicatedBlockDataAccess.TABLE_NAME,
            UnderReplicatedBlockDataAccess.BLOCK_ID, UnderReplicatedBlockDataAccess.LEVEL,
            UnderReplicatedBlockDataAccess.BLOCK_ID));
    log.info(String.format("Table %s is created.", UnderReplicatedBlockDataAccess.TABLE_NAME));
    
    
    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,"
            + "%s BIGINT NOT NULL,"
            + "%s BIGINT NOT NULL,"
            + "%s varchar(50) NOT NULL,"
            + "%s INTEGER NOT NULL,"
            + "%s INTEGER NOT NULL DEFAULT 0"
            + "PRIMARY KEY (%s,%S))", LeaderDataAccess.TABLE_NAME,
            LeaderDataAccess.ID, LeaderDataAccess.COUNTER, LeaderDataAccess.TIMESTAMP,
            LeaderDataAccess.HOSTNAME, LeaderDataAccess.AVG_REQUEST_PROCESSING_LATENCY,
            LeaderDataAccess.PARTITION_VAL, LeaderDataAccess.ID, LeaderDataAccess.PARTITION_VAL));
    
    log.info(String.format("Table %s is created.", UnderReplicatedBlockDataAccess.TABLE_NAME));

  }

  public void dropTables() throws StorageException {
    Connection conn = null;
    Statement s = null;
    try {
      conn = DriverManager.getConnection(protocol + ";create=false");

      s = conn.createStatement();
      s.execute(String.format("drop table %s", BlockInfoDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", CorruptReplicaDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", InodeDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", LeaseDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", LeasePathDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", PendingBlockDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", ReplicaDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", InvalidateBlockDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", ExcessReplicaDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", ReplicaUnderConstruntionDataAccess.TABLE_NAME));
      s.execute(String.format("drop table %s", UnderReplicatedBlockDataAccess.TABLE_NAME));
    } catch (SQLException ex) {
      log.warn("There is no table to remvoe or cannot remove the tables.");
    } finally {
      try {
        if (s != null && !s.isClosed()) {
          s.close();
          s = null;
        }
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
          conn = null;
        }
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
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
