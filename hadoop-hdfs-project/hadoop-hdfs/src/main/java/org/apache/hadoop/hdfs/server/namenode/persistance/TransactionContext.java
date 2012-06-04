package org.apache.hadoop.hdfs.server.namenode.persistance;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.Replica;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import se.sics.clusterj.BlockInfoTable;
import se.sics.clusterj.LeasePathsTable;
import se.sics.clusterj.LeaseTable;
import se.sics.clusterj.TripletsTable;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class TransactionContext {

  private static Log logger = LogFactory.getLog(TransactionContext.class);
  private boolean activeTxExpected = false;
  private boolean externallyMngedTx = true;
  private Map<Long, BlockInfo> blocks = new HashMap<Long, BlockInfo>();
  private Map<Long, BlockInfo> modifiedBlocks = new HashMap<Long, BlockInfo>();
  private Map<Long, BlockInfo> removedBlocks = new HashMap<Long, BlockInfo>();
  private Map<Long, List<BlockInfo>> inodeBlocks = new HashMap<Long, List<BlockInfo>>();
  private boolean allBlocksRead = false;
  private Map<String, Replica> modifiedReplicas = new HashMap<String, Replica>();
  private Map<String, Replica> removedReplicas = new HashMap<String, Replica>();
  private Map<Long, List<Replica>> blockReplicas = new HashMap<Long, List<Replica>>();
  private Map<Integer, TreeSet<LeasePath>> holderLeasePaths = new HashMap<Integer, TreeSet<LeasePath>>();
  private Map<LeasePath, LeasePath> leasePaths = new HashMap<LeasePath, LeasePath>();
  private Map<LeasePath, LeasePath> modifiedLPaths = new HashMap<LeasePath, LeasePath>();
  private Map<LeasePath, LeasePath> removedLPaths = new HashMap<LeasePath, LeasePath>();
  private Map<String, LeasePath> pathToLeasePath = new HashMap<String, LeasePath>();
  private Map<String, Lease> leases = new HashMap<String, Lease>();
  private Map<Integer, Lease> idToLease = new HashMap<Integer, Lease>();
  private Map<Lease, Lease> modifiedLeases = new HashMap<Lease, Lease>();
  private Map<Lease, Lease> removedLeases = new HashMap<Lease, Lease>();
  private boolean allLeasesRead = false;
  private boolean allLeasePathsRead = false;

  private void resetContext() {
    activeTxExpected = false;
    externallyMngedTx = true;
    
    blocks.clear();
    modifiedBlocks.clear();
    removedBlocks.clear();
    inodeBlocks.clear();
    allBlocksRead = false;
    
    modifiedReplicas.clear();
    removedReplicas.clear();
    blockReplicas.clear();
  }

  void begin() {
    activeTxExpected = true;
  }

  public void commit() throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Active transaction is expected.");
    }
    
    StringBuilder builder = new StringBuilder();
    
    Session session = DBConnector.obtainSession();
    for (BlockInfo block : removedBlocks.values()) {
      session.deletePersistent(BlockInfoTable.class, block.getBlockId());
      builder.append("rm Block:").append(block.getBlockId()).append("\n");
    }

    for (BlockInfo block : modifiedBlocks.values()) {
      BlockInfoTable newInstance = session.newInstance(BlockInfoTable.class);
      BlockInfoFactory.createPersistable(block, newInstance);
      session.savePersistent(newInstance);
      builder.append("w Block:").append(+ block.getBlockId()).append("\n");
    }

    for (Replica replica : removedReplicas.values()) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(TripletsTable.class, pk);
      builder.append("rm Replica:").append(replica.cacheKey()).append("\n");
    }

    for (Replica replica : modifiedReplicas.values()) {
      TripletsTable newInstance = session.newInstance(TripletsTable.class);
      ReplicaFactory.createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
      builder.append("w Replica:").append(replica.cacheKey()).append("\n");
    }

    logger.debug("Tx commit{ \n" + builder.toString() + "}");

    resetContext();
  }

  void rollback() {
    resetContext();

    logger.debug("Tx rollback");
  }

  public void persist(Object obj) throws TransactionContextException {
    if (!activeTxExpected) {
      throw new TransactionContextException("Transaction was not begun");
    }
    if (obj instanceof BlockInfo) {
      BlockInfo block = (BlockInfo) obj;

      if (removedBlocks.containsKey(block.getBlockId())) {
        throw new TransactionContextException("Removed block passed to be persisted");
      }

      BlockInfo contextInstance = blocks.get(block.getBlockId());

      if (logger.isDebugEnabled()) {
        if (contextInstance == null) {
          logger.debug("Block = " + block.getBlockId() + " is new instance");
        } else if (contextInstance == block) {
          logger.debug("Block = " + block.getBlockId() + " is mraked modified.");
        } else {
          logger.debug("Block =" + block.getBlockId() + " instance changed to " + ((block instanceof BlockInfoUnderConstruction) ? "UnderConstruction" : "BlockInfo"));
        }
      }

      blocks.put(block.getBlockId(), block);
      modifiedBlocks.put(block.getBlockId(), block);

    } else if (obj instanceof Replica) {
      Replica replica = (Replica) obj;

      if (removedReplicas.containsKey(replica.cacheKey())) {
        throw new TransactionContextException("Removed replica passed to be persisted");
      }

      modifiedReplicas.put(replica.cacheKey(), replica);
    } else if (obj instanceof LeasePath) {
      LeasePath lPath = (LeasePath) obj;

      if (removedLPaths.containsKey(lPath)) {
        throw new TransactionContextException("Removed lease-path passed to be persisted");
      }

      modifiedLPaths.put(lPath, lPath);
      leasePaths.put(lPath, lPath);
      pathToLeasePath.put(lPath.getPath(), lPath);
    } else if (obj instanceof Lease) {
      Lease lease = (Lease) obj;

      if (removedLeases.containsKey(lease)) {
        throw new TransactionContextException("Removed lease passed to be persisted");
      }

      modifiedLeases.put(lease, lease);
      leases.put(lease.getHolder(), lease);
      idToLease.put(lease.getHolderID(), lease);
    } else {
      throw new TransactionContextException("Unkown type passed for being persisted");
    }
  }

  public void remove(Object obj) throws TransactionContextException {
    beforeTxCheck();
    boolean done = true;

    try {
      if (obj instanceof BlockInfo) {
        BlockInfo block = (BlockInfo) obj;

        if (block.getBlockId() == 0l) {
          throw new TransactionContextException("Unassigned-Id block passed to be removed");
        }

        BlockInfo attachedBlock = blocks.get(block.getBlockId());

        if (attachedBlock == null) {
          throw new TransactionContextException("Unattached block passed to be removed");
        }

        blocks.remove(block.getBlockId());
        modifiedBlocks.remove(block.getBlockId());
        removedBlocks.put(block.getBlockId(), attachedBlock);

      }
      if (obj instanceof LeasePath) {
        LeasePath lPath = (LeasePath) obj;
        leasePaths.remove(lPath);
        pathToLeasePath.remove(lPath.getPath());
        modifiedLPaths.remove(lPath);
        removedLPaths.put(lPath, lPath);

      } else if (obj instanceof Lease) {
        Lease lease = (Lease) obj;

        if (!leases.containsKey(lease.getHolder())) {
          throw new TransactionContextException("Unattached lease passed to be removed");
        }

        leases.remove(lease.getHolder());
        idToLease.remove(lease.getHolderID());
        modifiedLeases.remove(lease);
        removedLeases.put(lease, lease);
      } else if (obj instanceof Replica) {
        Replica replica = (Replica) obj;

        modifiedReplicas.remove(replica.cacheKey());
        removedReplicas.put(replica.cacheKey(), replica);
      } else {
        done = false;
        throw new TransactionContextException("Unkown type passed for being persisted");
      }
    } finally {
      afterTxCheck(done);
    }
  }

  List<Replica> findReplicasByBlockId(long id) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (blockReplicas.containsKey(id)) {
        return blockReplicas.get(id);
      } else {
        Session session = DBConnector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
        dobj.where(dobj.get("blockId").equal(dobj.param("param")));
        Query<TripletsTable> query = session.createQuery(dobj);
        query.setParameter("param", id);
        List<TripletsTable> triplets = query.getResultList();
        List<Replica> replicas = ReplicaFactory.createReplicaList(triplets);
        blockReplicas.put(id, replicas);
        return replicas;
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public List<BlockInfo> findBlocksByInodeId(long id) throws IOException, TransactionContextException {
    beforeTxCheck();
    try {
      if (inodeBlocks.containsKey(id)) {
        return inodeBlocks.get(id);
      } else {
        Session session = DBConnector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
        dobj.where(dobj.get("iNodeID").equal(dobj.param("param")));
        Query<BlockInfoTable> query = session.createQuery(dobj);
        query.setParameter("param", id);
        List<BlockInfoTable> resultList = query.getResultList();
        List<BlockInfo> syncedList = syncBlockInfoInstances(BlockInfoFactory.createBlockInfoList(resultList));
        inodeBlocks.put(id, syncedList);
        return syncedList;
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public BlockInfo findBlockById(long blockId) throws IOException, TransactionContextException {
    beforeTxCheck();
    try {
      BlockInfo block = blocks.get(blockId);
      if (block == null) {
        Session session = DBConnector.obtainSession();
        BlockInfoTable bit = session.find(BlockInfoTable.class, blockId);
        if (bit == null) {
          return null;
        }
        block = BlockInfoFactory.createBlockInfo(bit);
        blocks.put(blockId, block);
      }
      return block;
    } finally {
      afterTxCheck(true);
    }
  }

  public List<BlockInfo> findAllBlocks() throws IOException, TransactionContextException {
    beforeTxCheck();
    try {
      if (allBlocksRead) {
        return new ArrayList<BlockInfo>(blocks.values());
      } else {
        Session session = DBConnector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
        Query<BlockInfoTable> query = session.createQuery(dobj);
        List<BlockInfoTable> resultList = query.getResultList();
        List<BlockInfo> syncedList = syncBlockInfoInstances(BlockInfoFactory.createBlockInfoList(resultList));
        allBlocksRead = true;
        return syncedList;
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public List<BlockInfo> findBlocksByStorageId(String name) throws IOException, TransactionContextException {
    beforeTxCheck();
    try {
      List<BlockInfo> ret = new ArrayList<BlockInfo>();
      Session session = org.apache.hadoop.hdfs.server.namenode.DBConnector.obtainSession();

      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
      dobj.where(dobj.get("storageId").equal(dobj.param("param")));
      Query<TripletsTable> query = session.createQuery(dobj);
      query.setParameter("param", name);
      List<TripletsTable> triplets = query.getResultList();

      for (TripletsTable t : triplets) {
        ret.add(findBlockById(t.getBlockId()));
      }
      return ret;
    } finally {
      afterTxCheck(true);
    }
  }

  private List<BlockInfo> syncBlockInfoInstances(List<BlockInfo> newBlocks) {
    List<BlockInfo> finalList = new ArrayList<BlockInfo>();

    for (BlockInfo blockInfo : newBlocks) {
      if (blocks.containsKey(blockInfo.getBlockId()) && !removedBlocks.containsKey(blockInfo.getBlockId())) {
        finalList.add(blocks.get(blockInfo.getBlockId()));
      } else {
        blocks.put(blockInfo.getBlockId(), blockInfo);
        finalList.add(blockInfo);
      }
    }

    return finalList;
  }

  private TreeSet<LeasePath> syncLeasePathInstances(List<LeasePathsTable> lpTables) {
    TreeSet<LeasePath> finalList = new TreeSet<LeasePath>();

    for (LeasePathsTable lpt : lpTables) {
      LeasePath lPath = LeasePathFactory.createLeasePath(lpt);
      if (this.leasePaths.containsKey(lPath)) {
        finalList.add(this.leasePaths.get(lPath));
      } else {
        this.leasePaths.put(lPath, lPath);
        this.pathToLeasePath.put(lpt.getPath(), lPath);
        finalList.add(lPath);
      }
    }

    return finalList;
  }

  private void beforeTxCheck() throws TransactionContextException {
    Session session = DBConnector.obtainSession();
    if (activeTxExpected && !session.currentTransaction().isActive()) {
      throw new TransactionContextException("Active transaction is expected.");
    } else if (!activeTxExpected) {
      DBConnector.beginTransaction();
      externallyMngedTx = false;
    }
  }

  private void afterTxCheck(boolean done) {
    if (!externallyMngedTx) {
      if (done) {
        DBConnector.commit();
      } else {
        DBConnector.safeRollback();
      }
    }
  }

  public TreeSet<LeasePath> findLeasePathsByHolderID(int holderID) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (holderLeasePaths.containsKey(holderID)) {
        return holderLeasePaths.get(holderID);
      } else {
        Session session = DBConnector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<LeasePathsTable> dobj = qb.createQueryDefinition(LeasePathsTable.class);
        dobj.where(dobj.get("holderID").equal(dobj.param("param")));
        Query<LeasePathsTable> query = session.createQuery(dobj);
        query.setParameter("param", holderID);
        List<LeasePathsTable> paths = query.getResultList();
        TreeSet<LeasePath> lpSet = syncLeasePathInstances(paths);
        holderLeasePaths.put(holderID, lpSet);

        return lpSet;
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public LeasePath findLeasePathByPath(String path) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (pathToLeasePath.containsKey(path)) {
        return pathToLeasePath.get(path);
      }

      Session session = DBConnector.obtainSession();
      LeasePathsTable lPTable = session.find(LeasePathsTable.class, path);
      LeasePath lPath = LeasePathFactory.createLeasePath(lPTable);
      leasePaths.put(lPath, lPath);
      pathToLeasePath.put(lPath.getPath(), lPath);

      return lPath;
    } finally {
      afterTxCheck(true);
    }
  }

  public TreeSet<LeasePath> findLeasePathsByPrefix(String prefix) throws TransactionContextException {
    beforeTxCheck();
    try {
      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeasePathsTable.class);
      PredicateOperand propertyPredicate = dobj.get("path");
      String param = "prefix";
      PredicateOperand propertyLimit = dobj.get(param);
      Predicate like = propertyPredicate.like(propertyLimit);
      dobj.where(like);
      Query query = session.createQuery(dobj);
      query.setParameter(param, prefix + "%");
      List<LeasePathsTable> resultset = query.getResultList();
      if (resultset != null) {
        return syncLeasePathInstances(resultset);
      }

      return null;
    } finally {
      afterTxCheck(true);
    }
  }

  public TreeSet<LeasePath> findAllLeasePaths() throws TransactionContextException {
    beforeTxCheck();

    try {
      if (allLeasePathsRead) {
        return new TreeSet<LeasePath>(leasePaths.values());
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeasePathsTable.class);
      Query query = session.createQuery(dobj);
      List<LeasePathsTable> resultset = query.getResultList();
      TreeSet<LeasePath> lPathSet = syncLeasePathInstances(resultset);
      allLeasePathsRead = true;
      return lPathSet;
    } finally {
      afterTxCheck(true);
    }
  }

  public Lease findLeaseByHolderId(int holderId) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (idToLease.containsKey(holderId)) {
        return idToLease.get(holderId);
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);

      dobj.where(dobj.get("holderID").equal(dobj.param("param")));

      Query<LeaseTable> query = session.createQuery(dobj);
      query.setParameter("param", holderId); //the WHERE clause of SQL
      List<LeaseTable> leaseTables = query.getResultList();

      if (leaseTables.size() > 1) {
        logger.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
        return null;
      } else if (leaseTables.size() == 1) {
        Lease lease = LeaseFactory.createLease(leaseTables.get(0));
        leases.put(lease.getHolder(), lease);
        idToLease.put(lease.getHolderID(), lease);
        return lease;
      } else {
        logger.info("No rows found for holderID:" + holderId + " in Lease table");
        return null;
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public Lease findLeaseByHolder(String holder) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (leases.containsKey(holder)) {
        return leases.get(holder);
      }

      Session session = DBConnector.obtainSession();
      LeaseTable lTable = session.find(LeaseTable.class, holder);
      if (lTable != null) {
        Lease lease = LeaseFactory.createLease(lTable);
        leases.put(lease.getHolder(), lease);
        return lease;
      }
      return null;
    } finally {
      afterTxCheck(true);
    }
  }

  /**
   * Finds the hard-limit expired leases. i.e. All leases older than the given time limit.
   * @param timeLimit
   * @return 
   */
  public SortedSet<Lease> findAllExpiredLeases(long timeLimit) throws TransactionContextException {
    beforeTxCheck();
    try {
      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(Lease.class);
      PredicateOperand propertyPredicate = dobj.get("lastUpdate");
      String param = "timelimit";
      PredicateOperand propertyLimit = dobj.get(param);
      Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
      dobj.where(lessThan);
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(timeLimit));
      List<LeaseTable> resultset = query.getResultList();
      return syncLeaseInstances(resultset);
    } finally {
      afterTxCheck(true);
    }
  }

  public SortedSet<Lease> findAllLeases() throws TransactionContextException {
    beforeTxCheck();
    try {
      if (allLeasesRead) {
        return new TreeSet<Lease>(this.leases.values());
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);
      Query<LeaseTable> query = session.createQuery(dobj);
      List<LeaseTable> resultList = query.getResultList();
      SortedSet<Lease> leaseSet = syncLeaseInstances(resultList);
      allLeasesRead = true;
      return leaseSet;
    } finally {
      afterTxCheck(true);
    }


  }

  private SortedSet<Lease> syncLeaseInstances(List<LeaseTable> lTables) {
    SortedSet<Lease> lSet = new TreeSet<Lease>();
    if (lTables != null) {
      for (LeaseTable lt : lTables) {
        Lease lease = LeaseFactory.createLease(lt);
        if (leases.containsKey(lease.getHolder()) && !removedLeases.containsKey(lease)) {
          lSet.add(leases.get(lease.getHolder()));
        } else {
          lSet.add(lease);
        }
      }
    }

    return lSet;
  }
}
