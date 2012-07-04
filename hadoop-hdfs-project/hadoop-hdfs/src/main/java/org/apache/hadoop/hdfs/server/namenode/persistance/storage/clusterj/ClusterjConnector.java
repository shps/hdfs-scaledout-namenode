package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.ClusterJException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.mysql.clusterj.ClusterJHelper;
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
  static SessionFactory[] sessionFactory;
  static Map<Long, Session> sessionPool = new ConcurrentHashMap<Long, Session>();
  static final Log LOG = LogFactory.getLog(ClusterjConnector.class);

  @Override
  public void setConfiguration(Configuration conf) {
    if (sessionFactory != null) {
      LOG.warn("SessionFactory is already initialized");
      return; //[W] workaround to prevent recreation of SessionFactory for the time being
    }
    NUM_SESSION_FACTORIES = conf.getInt(DFS_DB_NUM_SESSION_FACTORIES, 3);
    sessionFactory = new SessionFactory[NUM_SESSION_FACTORIES];
    LOG.info("Database connect string: " + conf.get(DFS_DB_CONNECTOR_STRING_KEY, DFS_DB_CONNECTOR_STRING_DEFAULT));
    LOG.info("Database name: " + conf.get(DFS_DB_DATABASE_KEY, DFS_DB_DATABASE_DEFAULT));
    for (int i = 0; i < NUM_SESSION_FACTORIES; i++) {
      Properties p = new Properties();
      p.setProperty("com.mysql.clusterj.connectstring", conf.get(DFS_DB_CONNECTOR_STRING_KEY, DFS_DB_CONNECTOR_STRING_DEFAULT));
      p.setProperty("com.mysql.clusterj.database", conf.get(DFS_DB_DATABASE_KEY, DFS_DB_DATABASE_DEFAULT));
      p.setProperty("com.mysql.clusterj.connection.pool.size", String.valueOf(NUM_SESSION_FACTORIES));
      sessionFactory[i] = ClusterJHelper.getSessionFactory(p);
    }
  }

  /*
   * Return a session from a random session factory in our pool.
   *
   * NOTE: Do not close the session returned by this call or you will die.
   */
  @Override
  public synchronized Session obtainSession() {
    long threadId = Thread.currentThread().getId();

    if (sessionPool.containsKey(threadId)) {
      return sessionPool.get(threadId);
    } else {
      // Pick a random sessionFactory
      Random r = new Random();
      LOG.info("New session object being obtained for threadId:" + threadId + " name:" + Thread.currentThread().getName());
      Session session = sessionFactory[r.nextInt(NUM_SESSION_FACTORIES)].getSession();
      sessionPool.put(threadId, session);
      return session;
    }
  }

  /**
   * begin a transaction.
   */
  @Override
  public void beginTransaction() {
    Session session = obtainSession();
//            session.setLockMode(LockMode.SHARED);
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
    try {
      tx.begin();
      session.deletePersistentAll(INodeTable.class);
      session.deletePersistentAll(BlockInfoTable.class);
      session.deletePersistentAll(LeaseTable.class);
      session.deletePersistentAll(LeasePathsTable.class);
      session.deletePersistentAll(IndexedReplicaTable.class);
      session.deletePersistentAll(ReplicaUcTable.class);
      session.deletePersistentAll(InvalidateBlocksTable.class);
      session.deletePersistentAll(ExcessReplicaTable.class);
      session.deletePersistentAll(PendingBlockTable.class);
      session.deletePersistentAll(CorruptReplicaTable.class);
      session.deletePersistentAll(UnderReplicatedBlocksTable.class);
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
}
