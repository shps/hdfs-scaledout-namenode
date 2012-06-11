package org.apache.hadoop.hdfs.server.namenode;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import se.sics.clusterj.UnderReplicaBlocksTable;

/**
 * This class would provide CRUD methods for persisting "UnderReplicatedBlocks" in BlockManager
 * 
 * From {UnderReplicatedBlocks}:
 * Keep track of under replication blocks.
 * Blocks have replication priority, with priority 0 indicating the highest
 * Blocks have only one replicas has the highest
 */
public class UnderReplicaBlocksHelper {

  private static Log LOG = LogFactory.getLog(UnderReplicaBlocksHelper.class);
  static final int RETRY_COUNT = 3;

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static boolean add(int level, long blockId, boolean isTransactional) {
    Session session = DBConnector.obtainSession();
    DBConnector.checkTransactionState(isTransactional);

    UnderReplicaBlocksTable ur = session.newInstance(UnderReplicaBlocksTable.class);
    ur.setLevel(level);
    ur.setBlockId(blockId);

    if (isTransactional) {
      try {
        insertBlockInternal(session, ur);
        return true;
      }
      catch (ClusterJException ex) {
        return false;
      }
    }
    else {
      return insertBlockWithTransaction(session, ur);
    }
  }

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private static boolean insertBlockWithTransaction(Session session, UnderReplicaBlocksTable ur) {
    Transaction tx = session.currentTransaction();

    int tries = RETRY_COUNT;
    boolean done = false;

    while (done == false && tries > 0) {
      try {
        tx.begin();
        insertBlockInternal(session, ur);
        tx.commit();
        done = true;
        return done;
      }
      catch (ClusterJException e) {
        tx.rollback();
        LOG.error("insertUnderReplicaBlockWithTransaction() failed " + e.getMessage(), e);
        tries--;
      }
    }
    return false;
  }
  
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static boolean remove(int level, long blockId, boolean isTransactional) {
    Session session = DBConnector.obtainSession();
    DBConnector.checkTransactionState(isTransactional);

    UnderReplicaBlocksTable ur = getBlockInternal(session, level, blockId);
    if (isTransactional) {
      try {
        removeBlockInternal(session, ur);
        return true;
      }
      catch (ClusterJException ex) {
        return false;
      }
    }
    else {
      return removeBlockWithTransaction(session, ur);
    }
  }
  
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private static boolean removeBlockWithTransaction(Session session, UnderReplicaBlocksTable ur) {
    Transaction tx = session.currentTransaction();

    int tries = RETRY_COUNT;
    boolean done = false;

    while (done == false && tries > 0) {
      try {
        tx.begin();
        removeBlockInternal(session, ur);
        tx.commit();
        done = true;
        return done;
      }
      catch (ClusterJException e) {
        tx.rollback();
        LOG.error("removeBlockWithTransaction() failed " + e.getMessage(), e);
        tries--;
      }
    }
    return false;
  }
  
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static void removeAll(boolean isTransactional) {
    Session session = DBConnector.obtainSession();
    DBConnector.checkTransactionState(isTransactional);

    if (isTransactional) {
      removeAllInternal(session);
    }
    else {
      removeAllWithTransaction(session);
    }
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private static void removeAllWithTransaction(Session session) {
    Transaction tx = session.currentTransaction();

    int tries = RETRY_COUNT;
    boolean done = false;

    while (done == false && tries > 0) {
      try {
        tx.begin();
        removeAllInternal(session);
        tx.commit();
        done = true;
      }
      catch (ClusterJException e) {
        tx.rollback();
        LOG.error("removeAllWithTransaction() failed " + e.getMessage(), e);
        tries--;
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static void update(int oldLevel, int newLevel, long blockId, boolean isTransactional) {
    remove(oldLevel, blockId, isTransactional);
    add(newLevel, blockId, isTransactional);
  }

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static List<NavigableSet<Block>> getAllBlocks(int totalLevels) {
    List<NavigableSet<Block>> priorityQueues = new ArrayList<NavigableSet<Block>>();

    for (int i = 0; i < totalLevels; i++) {
      priorityQueues.add(new TreeSet<Block>());
    }

    // Get all the under replicated blocks
    List<UnderReplicaBlocksTable> blocks = getBlocksInternal(DBConnector.obtainSession());

    for (int i = 0; i < blocks.size(); i++) {
      int level = blocks.get(i).getLevel();
      long blockId = blocks.get(i).getBlockId();
      priorityQueues.get(level).add(BlocksHelper.getBlock(blockId));
    }
    return priorityQueues;
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  public static int getTotalBlockCount() {
    return getBlocksInternal(DBConnector.obtainSession()).size();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  /**
   * Return the number of under replication blocks excluding corrupt blocks.
   * @param level indicates corrupted block level. This level stores all corrupted blocks
   */
  public static int getUnderReplicatedBlockCount(int level) {
    return getBlocksLessThanLevelInternal(DBConnector.obtainSession(), level).size();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  /**
   * Return the number of under replication blocks which are corrupt blocks.
   * @param level indicates corrupted block level. This level stores all corrupted blocks
   */
  public static int getCorruptedBlockSize(int level) {
    return getBlocksInternal(DBConnector.obtainSession(), level).size();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  public static boolean contains(long blockId) {
    return (getBlockInternal(DBConnector.obtainSession(), blockId).size() > 0);
  }

  ///////////////////////////////////////////////////////////////////// 
  /////////////////// Internal functions/////////////////////////////
  ///////////////////////////////////////////////////////////////////// 
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private static UnderReplicaBlocksTable getBlockInternal(Session session, int level, long blockId) {
    Object pk[] = {level, blockId};
    return session.find(UnderReplicaBlocksTable.class, pk);
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static List<UnderReplicaBlocksTable> getBlockInternal(Session session, long blockId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<UnderReplicaBlocksTable> dobj = qb.createQueryDefinition(UnderReplicaBlocksTable.class);
    Predicate pred = dobj.get("blockId").equal(dobj.param("blockId"));
    dobj.where(pred);
    Query<UnderReplicaBlocksTable> query = session.createQuery(dobj);
    query.setParameter("blockId", blockId);
    return query.getResultList();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static List<UnderReplicaBlocksTable> getBlocksInternal(Session session, int level) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<UnderReplicaBlocksTable> dobj = qb.createQueryDefinition(UnderReplicaBlocksTable.class);
    Predicate pred = dobj.get("level").equal(dobj.param("level"));
    dobj.where(pred);
    Query<UnderReplicaBlocksTable> query = session.createQuery(dobj);
    query.setParameter("level", level);
    return query.getResultList();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /*
   * Returns all blocks (usually for iteration)
   */

  private static List<UnderReplicaBlocksTable> getBlocksInternal(Session session) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<UnderReplicaBlocksTable> dobj = qb.createQueryDefinition(UnderReplicaBlocksTable.class);
    Query<UnderReplicaBlocksTable> query = session.createQuery(dobj);
    return query.getResultList();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static List<UnderReplicaBlocksTable> getBlocksLessThanLevelInternal(Session session, int level) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<UnderReplicaBlocksTable> dobj = qb.createQueryDefinition(UnderReplicaBlocksTable.class);
    Predicate pred = dobj.get("level").lessThan(dobj.param("level"));
    dobj.where(pred);
    Query<UnderReplicaBlocksTable> query = session.createQuery(dobj);
    query.setParameter("level", level);
    return query.getResultList();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static void insertBlockInternal(Session session, UnderReplicaBlocksTable ur) {
    session.savePersistent(ur);
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static void removeBlockInternal(Session session, UnderReplicaBlocksTable ur) {
    session.deletePersistent(ur);
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static void removeAllInternal(Session session) {
    session.deletePersistentAll(UnderReplicaBlocksTable.class);
  }
}
