package org.apache.hadoop.hdfs.server.namenode.persistance;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.Replica;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import se.sics.clusterj.BlockInfoTable;
import se.sics.clusterj.InvalidateBlocksTable;
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
  private boolean allInvBlocksRead = false;
  /**
   * InvalidatedBlocks
   */
  private Map<InvalidatedBlock, InvalidatedBlock> invBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private Map<String, List<InvalidatedBlock>> storageIdToInvBlocks = new HashMap<String, List<InvalidatedBlock>>();
  private Map<InvalidatedBlock, InvalidatedBlock> modifiedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private Map<InvalidatedBlock, InvalidatedBlock> removedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private long numInvBlocks = 0;

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
    
    invBlocks.clear();
    storageIdToInvBlocks.clear();
    modifiedInvBlocks.clear();
    removedInvBlocks.clear();
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
      builder.append("w Block:").append(+block.getBlockId()).append("\n");
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
    } else if (obj instanceof InvalidatedBlock) {
      InvalidatedBlock invBlock = (InvalidatedBlock) obj;

      if (removedInvBlocks.containsKey(invBlock)) {
        throw new TransactionContextException("Removed invalidated-block passed to be persisted");
      }

      invBlocks.put(invBlock, invBlock);
      modifiedInvBlocks.put(invBlock, invBlock);
      if (allBlocksRead)
        numInvBlocks++;
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

      } else if (obj instanceof Replica) {
        Replica replica = (Replica) obj;

        modifiedReplicas.remove(replica.cacheKey());
        removedReplicas.put(replica.cacheKey(), replica);
      }
      if (obj instanceof InvalidatedBlock) {
        InvalidatedBlock invBlock = (InvalidatedBlock) obj;

        invBlocks.remove(invBlock);
        modifiedInvBlocks.remove(invBlock);
        removedInvBlocks.put(invBlock, invBlock);
        if (allBlocksRead)
          numInvBlocks--;
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

  public List<InvalidatedBlock> findInvalidatedBlocksByStorageId(String storageId) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (storageIdToInvBlocks.containsKey(storageId)) {
        return this.storageIdToInvBlocks.get(storageId);
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksTable> qdt = qb.createQueryDefinition(InvalidateBlocksTable.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<InvalidateBlocksTable> query = session.createQuery(qdt);
      query.setParameter("param", storageId);
      List<InvalidateBlocksTable> invBlockTables = query.getResultList();
      List<InvalidatedBlock> invalidatedBlocks = syncInvalidatedBlockInstances(invBlockTables);
      return invalidatedBlocks;
    } finally {
      afterTxCheck(true);
    }
  }

  public InvalidatedBlock findInvalidatedBlockByPK(String storageId, long blockId) throws TransactionContextException {
    beforeTxCheck();
    try {
      InvalidatedBlock searchInstance = new InvalidatedBlock(storageId, blockId);
      if (invBlocks.containsKey(searchInstance)) {
        return invBlocks.get(searchInstance);
      }

      if (removedInvBlocks.containsKey(searchInstance)) {
        return null;
      }

      Session session = DBConnector.obtainSession();
      Object[] keys = new Object[2];
      keys[0] = storageId;
      keys[1] = blockId;
      InvalidateBlocksTable invTable = session.find(InvalidateBlocksTable.class, keys);
      InvalidatedBlock result = InvalidatedBlockFactory.createInvalidatedBlock(invTable);
      this.invBlocks.put(result, result);
      return result;
    } finally {
      afterTxCheck(true);
    }
  }

  public Map<String, List<InvalidatedBlock>> findAllInvalidatedBlocks() throws TransactionContextException {
    beforeTxCheck();
    try {
      if (allInvBlocksRead) {
        return storageIdToInvBlocks;
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksTable.class);
      List<InvalidateBlocksTable> ibts = session.createQuery(qdt).getResultList();
      numInvBlocks = 0;
      syncInvalidatedBlockInstances(ibts);

      allInvBlocksRead = true;

      return storageIdToInvBlocks;
    } finally {
      afterTxCheck(true);
    }
  }

  public long countAllInvalidatedBlocks() throws TransactionContextException {
    findAllInvalidatedBlocks();
    return numInvBlocks;
  }

  private List<InvalidatedBlock> syncInvalidatedBlockInstances(List<InvalidateBlocksTable> invBlockTables) {
    List<InvalidatedBlock> finalList = new ArrayList<InvalidatedBlock>();

    for (InvalidateBlocksTable bTable : invBlockTables) {
      InvalidatedBlock invBlock = InvalidatedBlockFactory.createInvalidatedBlock(bTable);
      if (!removedInvBlocks.containsKey(invBlock)) {
        numInvBlocks++;
        if (invBlocks.containsKey(invBlock)) {
          finalList.add(invBlocks.get(invBlock));
        } else {
          invBlocks.put(invBlock, invBlock);
          finalList.add(invBlock);
        }
        if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
          storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
        } else {
          List<InvalidatedBlock> ibList = new ArrayList<InvalidatedBlock>();
          ibList.add(invBlock);
          storageIdToInvBlocks.put(invBlock.getStorageId(), ibList);
        }
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
}
