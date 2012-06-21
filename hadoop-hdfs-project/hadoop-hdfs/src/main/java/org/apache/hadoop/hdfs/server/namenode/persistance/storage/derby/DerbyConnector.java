package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ExcessReplicaStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.InvalidatedBlockStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeasePathStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeaseStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.PendingBlockStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ReplicaUnderConstructionStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum DerbyConnector {

  INSTANCE;
  /* the default framework is embedded*/
  private String framework = "embedded";
  private String driver = "org.apache.derby.jdbc.EmbeddedDriver";
  private String protocol = "jdbc:derby:memory:";
  private String dbName = "derbyDB"; // the name of the database

  public void startDatabase() {

    System.out.println("Database is starting in " + framework + " mode");

    /* load the desired JDBC driver */
    loadDriver();

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
    } catch (SQLException ex) {
      Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
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

  public void stopDatabase() {
    if (framework.equals("embedded")) {
      try {
        // the shutdown=true attribute shuts down Derby
        DriverManager.getConnection("jdbc:derby:memory:;shutdown=true");

        // To shut down a specific database only, but keep the
        // engine running (for example for connecting to other
        // databases), specify a database in the connection URL:
        //DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true");
      } catch (SQLException se) {
        if (((se.getErrorCode() == 50000)
                && ("XJ015".equals(se.getSQLState())))) {
          // we got the expected exception
          System.out.println("Derby shut down normally");
          // Note that for single database shutdown, the expected
          // SQL state is "08006", and the error code is 45000.
        } else {
          // if the error code or SQLState is different, we have
          // an unexpected exception (shutdown failed)
          System.err.println("Derby did not shut down normally");
        }
      }
    }
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
            + "PRIMARY KEY (%s))", BlockInfoStorage.TABLE_NAME,
            BlockInfoStorage.BLOCK_ID, BlockInfoStorage.BLOCK_INDEX,
            BlockInfoStorage.INODE_ID, BlockInfoStorage.NUM_BYTES,
            BlockInfoStorage.GENERATION_STAMP, BlockInfoStorage.BLOCK_UNDER_CONSTRUCTION_STATE,
            BlockInfoStorage.TIME_STAMP, BlockInfoStorage.PRIMARY_NODE_INDEX,
            BlockInfoStorage.BLOCK_RECOVERY_ID, BlockInfoStorage.BLOCK_ID));
    System.out.println("Table block_info is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,"
            + "%s VARCHAR(128) NOT NULL,"
            + "PRIMARY KEY (%s,%s))", ExcessReplicaStorage.TABLE_NAME,
            ExcessReplicaStorage.BLOCK_ID, ExcessReplicaStorage.STORAGE_ID,
            ExcessReplicaStorage.BLOCK_ID, ExcessReplicaStorage.STORAGE_ID));
    System.out.println("Table ExcessReplica is created.");

    s.execute("CREATE TABLE inode ("
            + "id BIGINT NOT NULL,   "
            + "name varchar(128) DEFAULT NULL,   "
            + "parentid BIGINT DEFAULT NULL,   "
            + "isDir CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "modificationTime BIGINT DEFAULT NULL,   "
            + "aTime BIGINT DEFAULT NULL,   "
            + "permission VARCHAR (128) FOR BIT DATA DEFAULT NULL,   "
            + "nsquota BIGINT DEFAULT NULL,   "
            + "dsquota BIGINT DEFAULT NULL,   "
            + "isUnderConstruction CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "clientName varchar(45) DEFAULT NULL,   "
            + "clientMachine varchar(45) DEFAULT NULL,   "
            + "clientNode varchar(45) DEFAULT NULL,   "
            + "isClosedFile CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "header BIGINT DEFAULT NULL,   "
            + "isDirWithQuota CHAR (1) FOR BIT DATA DEFAULT NULL,   "
            + "nscount BIGINT DEFAULT NULL,   "
            + "dscount BIGINT DEFAULT NULL,   "
            + "symlink varchar(8000) DEFAULT NULL,  "
            + "PRIMARY KEY (id) )");
    System.out.println("Table inode is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "PRIMARY KEY (%s,%s) )", InvalidatedBlockStorage.TABLE_NAME,
            InvalidatedBlockStorage.BLOCK_ID, InvalidatedBlockStorage.STORAGE_ID,
            InvalidatedBlockStorage.GENERATION_STAMP, InvalidatedBlockStorage.NUM_BYTES,
            InvalidatedBlockStorage.BLOCK_ID, InvalidatedBlockStorage.STORAGE_ID));
    System.out.println("Table invalidated_block is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s varchar(255) NOT NULL,   "
            + "%s BIGINT DEFAULT NULL,   "
            + "%s INTEGER DEFAULT NULL,   "
            + "PRIMARY KEY (%s) )", LeaseStorage.TABLE_NAME,
            LeaseStorage.HOLDER, LeaseStorage.LAST_UPDATE, LeaseStorage.HOLDER_ID,
            LeaseStorage.HOLDER));
    System.out.println("Table lease is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s INTEGER NOT NULL,   "
            + "%s varchar(255) NOT NULL,   "
            + "PRIMARY KEY (%s) )", LeasePathStorage.TABLE_NAME,
            LeasePathStorage.HOLDER_ID, LeasePathStorage.PATH, LeasePathStorage.PATH));
    System.out.println("Table lease_path is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s BIGINT NOT NULL, %s BIGINT NOT NULL,"
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s) )", PendingBlockStorage.TABLE_NAME,
            PendingBlockStorage.BLOCK_ID, PendingBlockStorage.TIME_STAMP,
            PendingBlockStorage.NUM_REPLICAS_IN_PROGRESS, PendingBlockStorage.BLOCK_ID));
    System.out.println("Table pending_block is created.");

    s.execute(String.format("CREATE TABLE %s (   "
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s INTEGER DEFAULT NULL,   "
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s,%s) )", ReplicaUnderConstructionStorage.TABLE_NAME,
            ReplicaUnderConstructionStorage.BLOCK_ID, ReplicaUnderConstructionStorage.STORAGE_ID,
            ReplicaUnderConstructionStorage.STATE, ReplicaUnderConstructionStorage.REPLICA_INDEX,
            ReplicaUnderConstructionStorage.BLOCK_ID, ReplicaUnderConstructionStorage.STORAGE_ID));
    System.out.println("Table replica_uc is created.");

    s.execute(String.format("CREATE TABLE %s ("
            + "%s BIGINT NOT NULL,   "
            + "%s varchar(128) NOT NULL,   "
            + "%s INTEGER NOT NULL,   "
            + "PRIMARY KEY (%s,%s) )", IndexedReplicaStorage.TABLE_NAME,
            IndexedReplicaStorage.BLOCK_ID, IndexedReplicaStorage.STORAGE_ID,
            IndexedReplicaStorage.REPLICA_INDEX, IndexedReplicaStorage.BLOCK_ID,
            IndexedReplicaStorage.STORAGE_ID));
    System.out.println("Table tripletes is created.");
  }

  public Connection obtainSession() {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(protocol + dbName
              + ";create=true");
      conn.setAutoCommit(false);
    } catch (SQLException ex) {
      Logger.getLogger(DerbyConnector.class.getName()).log(Level.SEVERE, null, ex);
    }

    return conn;
  }
}
