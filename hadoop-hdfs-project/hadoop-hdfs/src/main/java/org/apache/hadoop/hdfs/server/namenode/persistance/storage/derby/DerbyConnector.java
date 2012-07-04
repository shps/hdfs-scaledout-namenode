package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.persistance.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ExcessReplicaContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.INodeContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.InvalidatedBlockContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeasePathContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeaseContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.PendingBlockContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ReplicaUnderConstructionContext;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum DerbyConnector implements StorageConnector<Connection> {

  INSTANCE;
  /* the default framework is embedded*/
  private String framework = "embedded";
  private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
  private String protocol = "jdbc:derby:memory:";
  private String dbName = "derbyDB"; // the name of the database
  private ThreadLocal<Connection> connectionPool = new ThreadLocal<Connection>();
  private ThreadLocal<Boolean> activeTransactions = new ThreadLocal<Boolean>();
  private boolean dbStarted = false;

  private DerbyConnector() {
    loadDriver();
  }

  @Override
  public synchronized void setConfiguration(Configuration conf) {
    if (!dbStarted) {
      startDatabase();
    }

  }

  @Override
  public Connection obtainSession() {
    Connection conn = this.connectionPool.get();
    if (conn == null) {
      try {
        conn = DriverManager.getConnection(protocol + dbName
                + ";create=false");
        conn.setAutoCommit(false);
        this.connectionPool.set(conn);
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

    return conn;
  }

  @Override
  public void beginTransaction() {
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
      Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      throw new StorageException(ex);
    } finally {
      this.activeTransactions.set(false);
    }
  }

  @Override
  public void rollback() {

    if (isTransactionActive()) {
      Connection connection = connectionPool.get();
      try {
        connection.rollback();
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      } finally {
        this.activeTransactions.set(false);
      }
    }

  }

  @Override
  public boolean formatStorage() {
//    startDatabase();
//    return true;
    Connection conn = null;
    Statement s = null;
    try {
      conn = DriverManager.getConnection(protocol + dbName
              + ";create=false");

      conn.setAutoCommit(false);

      s = conn.createStatement();
      s.execute(String.format("delete from %s", BlockInfoContext.TABLE_NAME));
      s.execute(String.format("delete from %s", INodeContext.TABLE_NAME));
      s.execute(String.format("delete from %s", LeaseContext.TABLE_NAME));
      s.execute(String.format("delete from %s", LeasePathContext.TABLE_NAME));
      s.execute(String.format("delete from %s", PendingBlockContext.TABLE_NAME));
      s.execute(String.format("delete from %s", IndexedReplicaContext.TABLE_NAME));
      s.execute(String.format("delete from %s", InvalidatedBlockContext.TABLE_NAME));
      s.execute(String.format("delete from %s", ExcessReplicaContext.TABLE_NAME));
      s.execute(String.format("delete from %s", ReplicaUnderConstructionContext.TABLE_NAME));

      //commit changes
      conn.commit();
      return true;
    } catch (SQLException ex) {
      Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      return false;
    } finally {
      try {
        if (s != null && !s.isClosed()) {
          s.close();
          s = null;
        }
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      }
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
          conn = null;
        }
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }

  @Override
  public boolean isTransactionActive() {
    return this.activeTransactions.get();
  }

  private void startDatabase() {
    dropTables();
    System.out.println("Database is starting in " + framework + " mode");

    /* load the desired JDBC driver */
    
    Connection conn = null;
    Statement s = null;
    try {
      /*
       * This connection specifies create=true in the connection URL to
       * cause the database to be created when connecting for the first
       * time. To remove the database, remove the directory derbyDB (the
       * same as the database name) and its contents.
       *
       * The directory derbyDB will be created under the directory that
       * the system property derby.system.home points to, or the current
       * directory (user.dir) if derby.system.home is not set.
       */
      conn = DriverManager.getConnection(protocol + dbName
              + ";create=true");

      System.out.println("Connected to and created database " + dbName);

      // We want to control transactions manually. Autocommit is on by
      // default in JDBC.
      conn.setAutoCommit(false);

      s = conn.createStatement();
      createTables(s);

      //commit changes
      conn.commit();
      dbStarted = true;
    } catch (SQLException ex) {
      Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      dbStarted = false;
    } finally {
      try {
        if (s != null && !s.isClosed()) {
          s.close();
          s = null;
        }
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      }
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
          conn = null;
        }
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }

  @Override
  public synchronized void stopStorage() {
//    dbStarted = false;
//    if (this.dbStarted) {
//      if (framework.equals("embedded")) {
//        try {
//          // the shutdown=true attribute shuts down Derby
//          DriverManager.getConnection(protocol + dbName + ";drop=true");
//          this.dbStarted = false;
//
//          // To shut down a specific database only, but keep the
//          // engine running (for example for connecting to other
//          // databases), specify a database in the connection URL:
//          //DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true");
//        } catch (SQLException se) {
//          if (((se.getErrorCode() == 50000)
//                  && ("XJ015".equals(se.getSQLState())))) {
//            // we got the expected exception
//            System.out.println("Derby shut down normally");
//            // Note that for single database shutdown, the expected
//            // SQL state is "08006", and the error code is 45000.
//          } else {
//            // if the error code or SQLState is different, we have
//            // an unexpected exception (shutdown failed)
//            System.err.println("Derby did not shut down normally");
//          }
//        }
//      }
//    }
  }

  /* Loads the appropriate JDBC driver for this environment/framework. For
   * example, if we are in an embedded environment, we load Derby's
   * embedded Driver, <code>org.apache.derby.jdbc.EmbeddedDriver</code>.
   */
  private void loadDriver() {
    /*
     *  The JDBC driver is loaded by loading its class.
     *  If you are using JDBC 4.0 (Java SE 6) or newer, JDBC drivers may
     *  be automatically loaded, making this code optional.
     *
     *  In an embedded environment, this will also start up the Derby
     *  engine (though not any databases), since it is not already
     *  running. In a client environment, the Derby engine is being run
     *  by the network server framework.
     *
     *  In an embedded environment, any static Derby system properties
     *  must be set before loading the driver to take effect.
     */
    try {
//      String driver2 = "org.apache.derby.jdbc.ClientDriver";
      Class.forName(driver).newInstance();
      System.out.println("Loaded the appropriate driver");
    } catch (ClassNotFoundException cnfe) {
      System.err.println("\nUnable to load the JDBC driver " + driver);
      System.err.println("Please check your CLASSPATH.");
      cnfe.printStackTrace(System.err);
    } catch (InstantiationException ie) {
      System.err.println(
              "\nUnable to instantiate the JDBC driver " + driver);
      ie.printStackTrace(System.err);
    } catch (IllegalAccessException iae) {
      System.err.println(
              "\nNot allowed to access the JDBC driver " + driver);
      iae.printStackTrace(System.err);
    }
  }

  private void createTables(Statement s) throws SQLException {
    System.out.println("Creating tables...");

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
            + "PRIMARY KEY (%s))", BlockInfoContext.TABLE_NAME,
            BlockInfoContext.BLOCK_ID, BlockInfoContext.BLOCK_INDEX,
            BlockInfoContext.INODE_ID, BlockInfoContext.NUM_BYTES,
            BlockInfoContext.GENERATION_STAMP, BlockInfoContext.BLOCK_UNDER_CONSTRUCTION_STATE,
            BlockInfoContext.TIME_STAMP, BlockInfoContext.PRIMARY_NODE_INDEX,
            BlockInfoContext.BLOCK_RECOVERY_ID, BlockInfoContext.BLOCK_ID));
    System.out.println("Table block_info is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,"
            + "%s VARCHAR(128) NOT NULL,"
            + "PRIMARY KEY (%s,%s))", ExcessReplicaContext.TABLE_NAME,
            ExcessReplicaContext.BLOCK_ID, ExcessReplicaContext.STORAGE_ID,
            ExcessReplicaContext.BLOCK_ID, ExcessReplicaContext.STORAGE_ID));
    System.out.println("Table ExcessReplica is created.");

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
            + "PRIMARY KEY (%s) )", INodeContext.TABLE_NAME,
            INodeContext.ID, INodeContext.NAME, INodeContext.PARENT_ID,
            INodeContext.IS_DIR, INodeContext.MODIFICATION_TIME,
            INodeContext.ACCESS_TIME, INodeContext.PERMISSION, INodeContext.NSQUOTA,
            INodeContext.DSQUOTA, INodeContext.IS_UNDER_CONSTRUCTION,
            INodeContext.CLIENT_NAME, INodeContext.CLIENT_MACHINE,
            INodeContext.CLIENT_NODE, INodeContext.IS_CLOSED_FILE,
            INodeContext.HEADER, INodeContext.IS_DIR_WITH_QUOTA,
            INodeContext.NSCOUNT, INodeContext.DSCOUNT, INodeContext.SYMLINK,
            INodeContext.ID));
    System.out.println("Table inode is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "PRIMARY KEY (%s,%s) )", InvalidatedBlockContext.TABLE_NAME,
            InvalidatedBlockContext.BLOCK_ID, InvalidatedBlockContext.STORAGE_ID,
            InvalidatedBlockContext.GENERATION_STAMP, InvalidatedBlockContext.NUM_BYTES,
            InvalidatedBlockContext.BLOCK_ID, InvalidatedBlockContext.STORAGE_ID));
    System.out.println("Table invalidated_block is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s varchar(255) NOT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s INTEGER DEFAULT NULL,   "
            + "PRIMARY KEY (%s) )", LeaseContext.TABLE_NAME,
            LeaseContext.HOLDER, LeaseContext.LAST_UPDATE, LeaseContext.HOLDER_ID,
            LeaseContext.HOLDER));
    System.out.println("Table lease is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s INTEGER NOT NULL,   "
            + "%s varchar(255) NOT NULL,   "
            + "PRIMARY KEY (%s) )", LeasePathContext.TABLE_NAME,
            LeasePathContext.HOLDER_ID, LeasePathContext.PATH, LeasePathContext.PATH));
    System.out.println("Table lease_path is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s BIGINT NOT NULL, %s BIGINT NOT NULL,"
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s) )", PendingBlockContext.TABLE_NAME,
            PendingBlockContext.BLOCK_ID, PendingBlockContext.TIME_STAMP,
            PendingBlockContext.NUM_REPLICAS_IN_PROGRESS, PendingBlockContext.BLOCK_ID));
    System.out.println("Table pending_block is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s INTEGER DEFAULT NULL,   "
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s,%s) )", ReplicaUnderConstructionContext.TABLE_NAME,
            ReplicaUnderConstructionContext.BLOCK_ID, ReplicaUnderConstructionContext.STORAGE_ID,
            ReplicaUnderConstructionContext.STATE, ReplicaUnderConstructionContext.REPLICA_INDEX,
            ReplicaUnderConstructionContext.BLOCK_ID, ReplicaUnderConstructionContext.STORAGE_ID));
    System.out.println("Table replica_uc is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s,%s) )", IndexedReplicaContext.TABLE_NAME,
            IndexedReplicaContext.BLOCK_ID, IndexedReplicaContext.STORAGE_ID,
            IndexedReplicaContext.REPLICA_INDEX, IndexedReplicaContext.BLOCK_ID,
            IndexedReplicaContext.STORAGE_ID));
    System.out.println("Table tripletes is created.");
  }
  
  private void dropTables()
  {
  Connection conn = null;
    Statement s = null;
    try {
      conn = DriverManager.getConnection(protocol + dbName
              + ";create=true");

      s = conn.createStatement();
      s.execute(String.format("drop table %s", BlockInfoContext.TABLE_NAME));
      s.execute(String.format("drop table %s", INodeContext.TABLE_NAME));
      s.execute(String.format("drop table %s", LeaseContext.TABLE_NAME));
      s.execute(String.format("drop table %s", LeasePathContext.TABLE_NAME));
      s.execute(String.format("drop table %s", PendingBlockContext.TABLE_NAME));
      s.execute(String.format("drop table %s", IndexedReplicaContext.TABLE_NAME));
      s.execute(String.format("drop table %s", InvalidatedBlockContext.TABLE_NAME));
      s.execute(String.format("drop table %s", ExcessReplicaContext.TABLE_NAME));
      s.execute(String.format("drop table %s", ReplicaUnderConstructionContext.TABLE_NAME));
    } catch (SQLException ex) {
      Logger.getLogger(DerbyConnector.class.getName()).log(Level.WARNING,
              "There is no table to remvoe or cannot remove the tables.");
    } finally {
      try {
        if (s != null && !s.isClosed()) {
          s.close();
          s = null;
        }
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.WARNING, null, ex);
      }
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
          conn = null;
        }
      } catch (SQLException ex) {
        Logger.getLogger(DerbyConnector.class.getName()).log(Level.WARNING, null, ex);
      }
    }
  }
}
