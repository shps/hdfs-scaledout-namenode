package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.ClusterJException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_CONNECTOR_STRING_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_CONNECTOR_STRING_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_DATABASE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_DATABASE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_NUM_SESSION_FACTORIES;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;

public enum ClusterjConnector implements StorageConnector<Session> {

  INSTANCE;
  private int NUM_SESSION_FACTORIES;
  static SessionFactory sessionFactory;
  static ThreadLocal<Session> sessionPool = new ThreadLocal<Session>();
  static final Log LOG = LogFactory.getLog(ClusterjConnector.class);

  @Override
  public void setConfiguration(Configuration conf) {
    if (sessionFactory != null) {
      LOG.warn("SessionFactory is already initialized");
      return;
    }
    NUM_SESSION_FACTORIES = conf.getInt(DFS_DB_NUM_SESSION_FACTORIES, 3);
    LOG.info("Database connect string: " + conf.get(DFS_DB_CONNECTOR_STRING_KEY, DFS_DB_CONNECTOR_STRING_DEFAULT));
    LOG.info("Database name: " + conf.get(DFS_DB_DATABASE_KEY, DFS_DB_DATABASE_DEFAULT));
    Properties p = new Properties();
    p.setProperty("com.mysql.clusterj.connectstring", conf.get(DFS_DB_CONNECTOR_STRING_KEY, DFS_DB_CONNECTOR_STRING_DEFAULT));
    p.setProperty("com.mysql.clusterj.database", conf.get(DFS_DB_DATABASE_KEY, DFS_DB_DATABASE_DEFAULT));
    p.setProperty("com.mysql.clusterj.connection.pool.size", String.valueOf(NUM_SESSION_FACTORIES));
    p.setProperty("com.mysql.clusterj.max.transactions", "1024");
    sessionFactory = ClusterJHelper.getSessionFactory(p);
  }

  /*
   * Return a session from a random session factory in our pool.
   *
   * NOTE: Do not close the session returned by this call or you will die.
   */
  @Override
  public Session obtainSession() {
    Session session = sessionPool.get();
    if (session == null) {
      LOG.info("New session object being obtained.");
      session = sessionFactory.getSession();
      sessionPool.set(session);
    }
    return session;
  }

  /**
   * begin a transaction.
   */
  @Override
  public void beginTransaction() {
    Session session = obtainSession();
    session.currentTransaction().begin();
  }

  /**
   * Commit a transaction.
   */
  @Override
  public void commit() throws StorageException {
    Session session = obtainSession();
    Transaction tx = session.currentTransaction();
    if (!tx.isActive()) {
      throw new StorageException("The transaction is not began!");
    }

    tx.commit();
    session.flush();
  }

  /**
   * It rolls back only when the transaction is active.
   */
  @Override
  public void rollback() {
    Session session = obtainSession();
    Transaction tx = session.currentTransaction();
    if (tx.isActive()) {
      tx.rollback();
    }
  }

  /**
   * This is called only when MiniDFSCluster wants to format the Namenode.
   */
  @Override
  public boolean formatStorage() {
    Session session = obtainSession();
    Transaction tx = session.currentTransaction();
    session.setLockMode(LockMode.READ_COMMITTED);
    try {
      tx.begin();
      session.deletePersistentAll(InodeClusterj.InodeDTO.class);
      session.deletePersistentAll(BlockInfoClusterj.BlockInfoDTO.class);
      session.deletePersistentAll(LeaseClusterj.LeaseDTO.class);
      session.deletePersistentAll(LeasePathClusterj.LeasePathsDTO.class);
      session.deletePersistentAll(ReplicaClusterj.ReplicaDTO.class);
      session.deletePersistentAll(ReplicaUnderConstructionClusterj.ReplicaUcDTO.class);
      session.deletePersistentAll(InvalidatedBlockClusterj.InvalidateBlocksDTO.class);
      session.deletePersistentAll(ExcessReplicaClusterj.ExcessReplicaDTO.class);
      session.deletePersistentAll(PendingBlockClusterj.PendingBlockDTO.class);
      session.deletePersistentAll(CorruptReplicaClusterj.CorruptReplicaDTO.class);
      session.deletePersistentAll(UnderReplicatedBlockClusterj.UnderReplicatedBlocksDTO.class);
      tx.commit();
      session.flush();
      return true;
    } catch (ClusterJException ex) {
      LOG.error(ex.getMessage(), ex);
      tx.rollback();
    }

    return false;
  }

  @Override
  public boolean isTransactionActive() {
    return obtainSession().currentTransaction().isActive();
  }

  @Override
  public void stopStorage() {
  }

  @Override
  public void readLock() {
    Session session = obtainSession();
    session.setLockMode(LockMode.SHARED);
  }

  @Override
  public void writeLock() {
    Session session = obtainSession();
    session.setLockMode(LockMode.EXCLUSIVE);
  }

  @Override
  public void readCommitted() {
    Session session = obtainSession();
    session.setLockMode(LockMode.READ_COMMITTED);
  }
}
