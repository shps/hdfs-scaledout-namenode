package org.apache.hadoop.hdfs.server.namenode;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import se.sics.clusterj.CorruptReplicasTable;

/** 
 * This class provides the CRUD methods for CorruptReplicas table
 * 
 * From {@link CorruptReplicasMap}:
 * 
 * Stores information about all corrupt blocks in the File System.
 * A Block is considered corrupt only if all of its replicas are corrupt. 
 * While reporting replicas of a Block, we hide any corrupt
 * copies. These copies are removed once Block is found to have 
 * expected number of good replicas.
 * Mapping: Block -> TreeSet<DatanodeDescriptor> 

 * */
public class CorruptReplicasHelper {

  private static Log LOG = LogFactory.getLog(CorruptReplicasHelper.class);
  static final int RETRY_COUNT = 3;

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /*
   * Adds a corrupt replica for this block
   */
  public static void addToCorruptReplicas(Block blk, DatanodeDescriptor dn, boolean isTransactional) {
    Session session = DBConnector.obtainSession();
    DBConnector.checkTransactionState(isTransactional);

    CorruptReplicasTable creplica = session.newInstance(CorruptReplicasTable.class);
    creplica.setBlockId(blk.getBlockId());
    creplica.setStorageId(dn.getStorageID());

    if (isTransactional) {
      insertCorruptReplicaInternal(session, creplica);
    }
    else {
      insertCorruptReplicaWithTransaction(session, creplica);
    }
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static void insertCorruptReplicaWithTransaction(Session session, CorruptReplicasTable creplica) {
    Transaction tx = session.currentTransaction();

    int tries = RETRY_COUNT;
    boolean done = false;

    while (done == false && tries > 0) {
      try {
        tx.begin();
        insertCorruptReplicaInternal(session, creplica);
        tx.commit();
        done = true;
      }
      catch (ClusterJException e) {
        tx.rollback();
        LOG.error("insertCorruptReplicaInternalWithTransaction() failed " + e.getMessage(), e);
        tries--;
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /*
   * Removes the block from list of corrupt blocks (because it has no corrupt replicas)
   */
  public static int removeFromCorruptReplicas(Block blk, boolean isTransactional) {
    Session session = DBConnector.obtainSession();
    DBConnector.checkTransactionState(isTransactional);

    List<CorruptReplicasTable> creplicas = getCorruptReplicaInternal(session, blk);
    if (isTransactional) {
      return removeCorruptReplicaInternal(session, creplicas);
    }
    else {
      return removeCorruptReplicaWithTransaction(session, creplicas);
    }
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /*
   * Removes a replica that was corrupt for that block
   */

  public static int removeFromCorruptReplicas(Block blk, DatanodeDescriptor dn, boolean isTransactional) {
    Session session = DBConnector.obtainSession();
    DBConnector.checkTransactionState(isTransactional);

    CorruptReplicasTable creplica = getCorruptReplicaInternal(session, blk, dn);
    List<CorruptReplicasTable> creplicas = new ArrayList<CorruptReplicasTable>();
    creplicas.add(creplica);

    if (isTransactional) {
      return removeCorruptReplicaInternal(session, creplicas);
    }
    else {
      return removeCorruptReplicaWithTransaction(session, creplicas);
    }
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static int removeCorruptReplicaWithTransaction(Session session, List<CorruptReplicasTable> creplicas) {
    Transaction tx = session.currentTransaction();

    int tries = RETRY_COUNT;
    boolean done = false;
    int recordsDeleted = 0;

    while (done == false && tries > 0) {
      try {
        tx.begin();
        recordsDeleted = removeCorruptReplicaInternal(session, creplicas);
        tx.commit();
        done = true;
      }
      catch (ClusterJException e) {
        tx.rollback();
        LOG.error("removeCorruptReplicaWithTransaction() failed " + e.getMessage(), e);
        tries--;
      }
    }
    return recordsDeleted;
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  public static Collection<DatanodeDescriptor> getNodes(Block blk) {
    Collection<DatanodeDescriptor> datanodes = new TreeSet<DatanodeDescriptor>();

    // get all corrupt replica for this block from db
    List<CorruptReplicasTable> creplicas = getCorruptReplicaInternal(DBConnector.obtainSession(), blk);
    for (CorruptReplicasTable c : creplicas) {
      // Fill the dnd data
      datanodes.add(DatanodeHelper.getDatanodeDescriptorByStorageId(c.getStorageId()));
    }

    return datanodes;
  }

  //Check if replica belonging to Datanode is corrupt
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static boolean isReplicaCorrupt(Block blk, DatanodeDescriptor dn) {
    CorruptReplicasTable creplica = getCorruptReplicaInternal(DBConnector.obtainSession(), blk, dn);
    if (creplica == null) {
      // Not persisted, so not replicated
      return false;
    }
    else {
      return true;
    }
  }

  //Check if replica belonging to Datanode is corrupt
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static SortedSet<Long> getCorruptBlocks() {
    return getCorruptBlocksInternal(DBConnector.obtainSession());
  }
  ///////////////////////////////////////////////////////////////////// 
  /////////////////// Internal functions/////////////////////////////
  ///////////////////////////////////////////////////////////////////// 
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static CorruptReplicasTable getCorruptReplicaInternal(Session session, Block blk, DatanodeDescriptor dn) {
    Object pk[] = {blk.getBlockId(), dn.getStorageID()};
    return session.find(CorruptReplicasTable.class, pk);
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static List<CorruptReplicasTable> getCorruptReplicaInternal(Session session, Block blk) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<CorruptReplicasTable> dobj = qb.createQueryDefinition(CorruptReplicasTable.class);
    Predicate pred = dobj.get("blockId").equal(dobj.param("blockId"));
    dobj.where(pred);
    Query<CorruptReplicasTable> query = session.createQuery(dobj);
    query.setParameter("blockId", blk.getBlockId());
    return query.getResultList();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  // No where clause (select all corrupt blocks)
  private static SortedSet<Long> getCorruptBlocksInternal(Session session) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<CorruptReplicasTable> dobj = qb.createQueryDefinition(CorruptReplicasTable.class);
    Query<CorruptReplicasTable> query = session.createQuery(dobj);

    // We want just the distinct block ids. All rows will be returned from the above query
    // These rows will have duplicate block ids since we have a composite pk (i.e. blockId and storageId)
    SortedSet<Long> blocks = new TreeSet<Long>();
    for (CorruptReplicasTable c : query.getResultList()) {
      blocks.add(c.getBlockId());
    }
    return blocks;
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static void insertCorruptReplicaInternal(Session session, CorruptReplicasTable creplica) {
    session.savePersistent(creplica);
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

  private static int removeCorruptReplicaInternal(Session session, List<CorruptReplicasTable> creplica) {
    session.deletePersistent(creplica);
    return creplica.size();
  }
}
