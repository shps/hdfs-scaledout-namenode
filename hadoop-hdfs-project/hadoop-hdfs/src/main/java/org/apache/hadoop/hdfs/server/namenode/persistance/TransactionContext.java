package org.apache.hadoop.hdfs.server.namenode.persistance;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.DatanodeHelper;
import se.sics.clusterj.BlockInfoTable;
import se.sics.clusterj.CorruptReplicasTable;
import se.sics.clusterj.ExcessReplicaTable;
import se.sics.clusterj.InvalidateBlocksTable;
import se.sics.clusterj.TripletsTable;
import se.sics.clusterj.UnderReplicaBlocksTable;

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
  private Map<String, IndexedReplica> modifiedReplicas = new HashMap<String, IndexedReplica>();
  private Map<String, IndexedReplica> removedReplicas = new HashMap<String, IndexedReplica>();
  private Map<Long, List<IndexedReplica>> blockReplicas = new HashMap<Long, List<IndexedReplica>>();
  /**
   * InvalidatedBlocks
   */
  private Map<InvalidatedBlock, InvalidatedBlock> invBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private Map<String, HashSet<InvalidatedBlock>> storageIdToInvBlocks = new HashMap<String, HashSet<InvalidatedBlock>>();
  private Map<InvalidatedBlock, InvalidatedBlock> modifiedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private Map<InvalidatedBlock, InvalidatedBlock> removedInvBlocks = new HashMap<InvalidatedBlock, InvalidatedBlock>();
  private boolean allInvBlocksRead = false;
  /**
   * ExcessReplica
   */
  private Map<ExcessReplica, ExcessReplica> exReplicas = new HashMap<ExcessReplica, ExcessReplica>();
  private Map<String, TreeSet<Long>> storageIdToExReplica = new HashMap<String, TreeSet<Long>>();
  private Map<ExcessReplica, ExcessReplica> modifiedExReplica = new HashMap<ExcessReplica, ExcessReplica>();
  private Map<ExcessReplica, ExcessReplica> removedExReplica = new HashMap<ExcessReplica, ExcessReplica>();

  /**
   * CorruptReplicas
       * A block that is corrupt means that one of its replica is corrupt
       * So each time the namenode detects a corrupt replica either from Datanode or Client or by itself, it will add this replica to the list of corrupt replica
   */
  private Map<CorruptReplica, CorruptReplica> corruptReplicas = new HashMap<CorruptReplica, CorruptReplica>();
  private Map<CorruptReplica, CorruptReplica> modifiedCorruptReplicas = new HashMap<CorruptReplica, CorruptReplica>();
  private Map<CorruptReplica, CorruptReplica> removedCorruptReplicas = new HashMap<CorruptReplica, CorruptReplica>();
  private long numCorruptBlocks = 0;
  private boolean allCorruptBlocksRead = false;

  /**
   * Under replicated blocks
       * An under replicated block has a priority level with 0 as the highest
       * Blocks have only one replicas has the highest
   */
  private Map<UnderReplicatedBlock, UnderReplicatedBlock> urBlocks = new HashMap<UnderReplicatedBlock, UnderReplicatedBlock>();
  private Map<UnderReplicatedBlock, UnderReplicatedBlock> modifiedurBlocks = new HashMap<UnderReplicatedBlock, UnderReplicatedBlock>();
  private Map<UnderReplicatedBlock, UnderReplicatedBlock> removedurBlocks = new HashMap<UnderReplicatedBlock, UnderReplicatedBlock>();
  private long numUrBlocks = 0;
  private boolean allUrBlocksRead = false;

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
    allInvBlocksRead = false;

    exReplicas.clear();
    storageIdToExReplica.clear();
    modifiedExReplica.clear();
    removedExReplica.clear();
    
    corruptReplicas.clear();
    modifiedCorruptReplicas.clear();
    removedCorruptReplicas.clear();
    numCorruptBlocks = 0;
    allCorruptBlocksRead = false;
    
    urBlocks.clear();
    modifiedurBlocks.clear();
    removedurBlocks.clear();
    numUrBlocks = 0;
    allUrBlocksRead = false;
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

    for (IndexedReplica replica : removedReplicas.values()) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(TripletsTable.class, pk);
      builder.append("rm Replica:").append(replica.cacheKey()).append("\n");
    }

    for (IndexedReplica replica : modifiedReplicas.values()) {
      TripletsTable newInstance = session.newInstance(TripletsTable.class);
      IndexedReplicaFactory.createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
      builder.append("w Replica:").append(replica.cacheKey()).append("\n");
    }

    for (InvalidatedBlock invBlock : modifiedInvBlocks.values()) {
      InvalidateBlocksTable newInstance = session.newInstance(InvalidateBlocksTable.class);
      ReplicaFactory.createPersistable(invBlock, newInstance);
      session.savePersistent(newInstance);
      builder.append("w InvalidatedBlock:").append(invBlock.toString()).append("\n");
    }

    for (InvalidatedBlock invBlock : removedInvBlocks.values()) {
      Object[] pk = new Object[2];
      pk[0] = invBlock.getBlockId();
      pk[1] = invBlock.getStorageId();
      session.deletePersistent(InvalidateBlocksTable.class, pk);
      builder.append("rm InvalidatedBlock:").append(invBlock.toString()).append("\n");
    }

    for (ExcessReplica exReplica : modifiedExReplica.values()) {
      ExcessReplicaTable newInstance = session.newInstance(ExcessReplicaTable.class);
      ReplicaFactory.createPersistable(exReplica, newInstance);
      session.savePersistent(newInstance);
      builder.append("w ExcessReplica:").append(exReplica.toString()).append("\n");
    }

    for (ExcessReplica exReplica : removedExReplica.values()) {
      Object[] pk = new Object[2];
      pk[0] = exReplica.getBlockId();
      pk[1] = exReplica.getStorageId();
      session.deletePersistent(ExcessReplicaTable.class, pk);
      builder.append("rm ExcessReplica:").append(exReplica.toString()).append("\n");
    }

    for (CorruptReplica corruptReplica : removedCorruptReplicas.values()) {
      Object[] pk = new Object[2];
      pk[0] = corruptReplica.getBlockId();
      pk[1] = corruptReplica.getStorageId();
      session.deletePersistent(CorruptReplicasTable.class, pk);
      builder.append("rm CorruptReplica:").append("[blk-").append(pk[0]).append("][dn-").append(pk[1]).append("]").append("\n");
    }
    for (CorruptReplica corruptReplica : modifiedCorruptReplicas.values()) {
        CorruptReplicasTable newInstance = session.newInstance(CorruptReplicasTable.class);
        ReplicaFactory.createPersistable(corruptReplica, newInstance);
        session.savePersistent(newInstance);
        builder.append("w CorruptReplica:").append("[blk-").append(corruptReplica.getBlockId()).append("][dn-").append(corruptReplica.getStorageId()).append("]").append("\n");
    }
    
    for (UnderReplicatedBlock urBlock : removedurBlocks.values()) {
      session.deletePersistent(UnderReplicaBlocksTable.class, urBlock.getBlockId());
      builder.append("rm UnderReplicated block:").append("[blk-").append(urBlock.getBlockId()).append("][level-").append(urBlock.getLevel()).append("]").append("\n");
    }
    for (UnderReplicatedBlock urBlock : modifiedurBlocks.values()) {
        UnderReplicaBlocksTable newInstance = session.newInstance(UnderReplicaBlocksTable.class);
        ReplicaFactory.createPersistable(urBlock, newInstance);
        session.savePersistent(newInstance);
        builder.append("w UnderReplicated block:").append("[blk-").append(urBlock.getBlockId()).append("][level-").append(urBlock.getLevel()).append("]").append("\n");
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
        }
        else if (contextInstance == block) {
          logger.debug("Block = " + block.getBlockId() + " is mraked modified.");
        }
        else {
          logger.debug("Block =" + block.getBlockId() + " instance changed to " + ((block instanceof BlockInfoUnderConstruction) ? "UnderConstruction" : "BlockInfo"));
        }
      }

      blocks.put(block.getBlockId(), block);
      modifiedBlocks.put(block.getBlockId(), block);

    }
    else if (obj instanceof IndexedReplica) {
      IndexedReplica replica = (IndexedReplica) obj;

      if (removedReplicas.containsKey(replica.cacheKey())) {
        throw new TransactionContextException("Removed replica passed to be persisted");
      }

      modifiedReplicas.put(replica.cacheKey(), replica);
    }
    else if (obj instanceof InvalidatedBlock) {
      InvalidatedBlock invBlock = (InvalidatedBlock) obj;

      if (removedInvBlocks.containsKey(invBlock)) {
        throw new TransactionContextException("Removed invalidated-block passed to be persisted");
      }

      addStorageToInvalidatedBlock(invBlock);

      invBlocks.put(invBlock, invBlock);
      modifiedInvBlocks.put(invBlock, invBlock);
    } else if (obj instanceof ExcessReplica) {
      ExcessReplica exReplica = (ExcessReplica) obj;

      if (removedExReplica.containsKey(exReplica)) {
        throw new TransactionContextException("Removed excess-replica passed to be persisted");
      }

      exReplicas.put(exReplica, exReplica);
      modifiedExReplica.put(exReplica, exReplica);
    }
    else if (obj instanceof CorruptReplica) {
      CorruptReplica corruptreplica = (CorruptReplica) obj;
      if (removedCorruptReplicas.get(corruptreplica) != null) {
        throw new TransactionContextException("Removed corrupt replica passed to be persisted");
      }

      corruptReplicas.put(corruptreplica, corruptreplica);
      modifiedCorruptReplicas.put(corruptreplica, corruptreplica);
    }
    else if (obj instanceof UnderReplicatedBlock) {
      UnderReplicatedBlock urBlock = (UnderReplicatedBlock) obj;
      if (removedurBlocks.get(urBlock) != null) {
        throw new TransactionContextException("Removed under replica passed to be persisted");
      }

      urBlocks.put(urBlock, urBlock);
      modifiedurBlocks.put(urBlock, urBlock);
    }
    else {
      throw new TransactionContextException("Unkown type passed for being persisted");
    }
  }

  private void addStorageToInvalidatedBlock(InvalidatedBlock invBlock) {
    if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
      storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
    } else {
      HashSet<InvalidatedBlock> invBlockList = new HashSet<InvalidatedBlock>();
      invBlockList.add(invBlock);
      storageIdToInvBlocks.put(invBlock.getStorageId(), invBlockList);
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

      } else if (obj instanceof IndexedReplica) {
        IndexedReplica replica = (IndexedReplica) obj;

        modifiedReplicas.remove(replica.cacheKey());
        removedReplicas.put(replica.cacheKey(), replica);
      } else if (obj instanceof InvalidatedBlock) {
        InvalidatedBlock invBlock = (InvalidatedBlock) obj;

        if (!invBlocks.containsKey(invBlock)) {
          throw new TransactionContextException("Unattached invalidated-block passed to be removed");
        }

        invBlocks.remove(invBlock);
        modifiedInvBlocks.remove(invBlock);
        removedInvBlocks.put(invBlock, invBlock);
        if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
          HashSet<InvalidatedBlock> ibs = storageIdToInvBlocks.get(invBlock.getStorageId());
          ibs.remove(invBlock);
          if (ibs.isEmpty()) {
            storageIdToInvBlocks.remove(invBlock.getStorageId());
          }
        }
      } else if (obj instanceof ExcessReplica) {
        ExcessReplica exReplica = (ExcessReplica) obj;

        if (exReplicas.remove(exReplica) == null) {
          throw new TransactionContextException("Unattached excess-replica passed to be removed");
        }

        modifiedExReplica.remove(exReplica);
        removedExReplica.put(exReplica, exReplica);
      } else if(obj instanceof CorruptReplica) {
        CorruptReplica corruptReplica = (CorruptReplica) obj;
        
        //if(corruptReplicas.get(corruptReplica) == null) {
        //  throw new TransactionContextException("Unattached corrupt replica passed to be removed");
       // }
        corruptReplicas.remove(corruptReplica);
        modifiedCorruptReplicas.remove(corruptReplica);
        removedCorruptReplicas.put(corruptReplica, corruptReplica);
          
      }
      else if(obj instanceof UnderReplicatedBlock) {
        UnderReplicatedBlock urBlock = (UnderReplicatedBlock) obj;
        
        if(!urBlocks.containsKey(urBlock)) {
          throw new TransactionContextException("Unattached under replica [blk:"+urBlock.getBlockId()+", level: "+urBlock.getLevel()+" ] passed to be removed");
        }
        urBlocks.remove(urBlock);
        modifiedurBlocks.remove(urBlock);
        removedurBlocks.put(urBlock, urBlock);
          
      } else {
        done = false;
        throw new TransactionContextException("Unkown type passed for being persisted");
      }
    } finally {
      afterTxCheck(done);
    }
  }

  public void removeAll(Class type) throws TransactionContextException {
    beforeTxCheck();
    boolean done = false;
    try {
        if(type.equals(UnderReplicatedBlock.class)) {
          // Clear all maps related to UnderReplicatedBlock
          removedurBlocks.clear();
          urBlocks.clear();
          modifiedurBlocks.clear();
          
          // Delete from Db
          DBConnector.obtainSession().deletePersistentAll(UnderReplicaBlocksTable.class);
          done = true;
      }else {
        done = false;
        throw new TransactionContextException("Unkown type passed for being persisted");
      }
    } finally {
      afterTxCheck(done);
    }
  }

  public void update(Object newValue) throws TransactionContextException {
    beforeTxCheck();
    boolean done = false;
    try {
      if (newValue instanceof UnderReplicatedBlock) {
        UnderReplicatedBlock urBlockNew = (UnderReplicatedBlock) newValue;

        // Update the maps i.e. put old value into removed and put new values into the maps
        if(urBlocks.containsKey(urBlockNew)) {
          urBlocks.put(urBlockNew, urBlockNew);
        }
        else {
          throw new TransactionContextException("Unattached under replica to be updated");
        }
        
        if(modifiedurBlocks.containsKey(urBlockNew)) {
          modifiedurBlocks.put(urBlockNew, urBlockNew);
        }
        else {
          throw new TransactionContextException("Unattached under replica to be updated");
        }
        done = true;
      }
      else {
        done = false;
        throw new TransactionContextException("Unkown type passed for being persisted");
      }
    }
    finally {
      afterTxCheck(done);
    }
  }

  List<IndexedReplica> findReplicasByBlockId(long id) throws TransactionContextException {
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
        List<IndexedReplica> replicas = IndexedReplicaFactory.createReplicaList(triplets);
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
        return new ArrayList<InvalidatedBlock>(this.storageIdToInvBlocks.get(storageId)); //clone the list reference
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksTable> qdt = qb.createQueryDefinition(InvalidateBlocksTable.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<InvalidateBlocksTable> query = session.createQuery(qdt);
      query.setParameter("param", storageId);
      List<InvalidateBlocksTable> invBlockTables = query.getResultList();
      syncInvalidatedBlockInstances(invBlockTables);
      HashSet<InvalidatedBlock> ibSet = storageIdToInvBlocks.get(storageId);
      if (ibSet != null) {
        return new ArrayList(ibSet);
      } else {
        return new ArrayList<InvalidatedBlock>();
      }
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
      keys[0] = blockId;
      keys[1] = storageId;
      InvalidateBlocksTable invTable = session.find(InvalidateBlocksTable.class, keys);
      if (invTable == null) {
        return null;
      }

      InvalidatedBlock result = ReplicaFactory.createReplica(invTable);
      this.invBlocks.put(result, result);
      return result;
    } finally {
      afterTxCheck(true);
    }
  }

  public Map<String, HashSet<InvalidatedBlock>> findAllInvalidatedBlocks() throws TransactionContextException {
    beforeTxCheck();
    try {
      if (allInvBlocksRead) {
        return storageIdToInvBlocks;
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksTable.class);
      List<InvalidateBlocksTable> ibts = session.createQuery(qdt).getResultList();
      syncInvalidatedBlockInstances(ibts);

      allInvBlocksRead = true;

      return storageIdToInvBlocks;
    } finally {
      afterTxCheck(true);
    }
  }

  public long countAllInvalidatedBlocks() throws TransactionContextException {
    findAllInvalidatedBlocks();
    long count = 0;
    for (HashSet ibset : storageIdToInvBlocks.values()) {
      count += ibset.size();
    }
    return count;
  }

  private void syncInvalidatedBlockInstances(List<InvalidateBlocksTable> invBlockTables) {
    for (InvalidateBlocksTable bTable : invBlockTables) {
      InvalidatedBlock invBlock = ReplicaFactory.createReplica(bTable);
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
        } else {
          invBlocks.put(invBlock, invBlock);
        }
        addStorageToInvalidatedBlock(invBlock);
      }
    }
  }

  public TreeSet<Long> findExcessReplicaByStorageId(String storageId) throws TransactionContextException {
    beforeTxCheck();
    try {
      if (storageIdToExReplica.containsKey(storageId)) {
        return storageIdToExReplica.get(storageId);
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ExcessReplicaTable> qdt = qb.createQueryDefinition(ExcessReplicaTable.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<ExcessReplicaTable> query = session.createQuery(qdt);
      query.setParameter("param", storageId);
      List<ExcessReplicaTable> invBlockTables = query.getResultList();
      TreeSet<Long> exReplicaSet = syncExcessReplicaInstances(invBlockTables);
      storageIdToExReplica.put(storageId, exReplicaSet);

      return exReplicaSet;
    } finally {
      afterTxCheck(true);
    }
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

  private TreeSet<Long> syncExcessReplicaInstances(List<ExcessReplicaTable> exReplicaTables) {
    TreeSet<Long> replicaSet = new TreeSet<Long>();

    if (exReplicaTables != null) {
      for (ExcessReplicaTable ert : exReplicaTables) {
        ExcessReplica replica = ReplicaFactory.createReplica(ert);
        if (!removedExReplica.containsKey(replica)) {
          if (exReplicas.containsKey(replica)) {
            replicaSet.add(exReplicas.get(replica).getBlockId());
          } else {
            exReplicas.put(replica, replica);
            replicaSet.add(replica.getBlockId());
          }
        }
      }
    }

    return replicaSet;
  }

  /**
   * This method is only used for metrics.
   * @return
   * @throws TransactionContextException 
   */
  public long countAllExcessReplicas() throws TransactionContextException {
    beforeTxCheck();
    try {
      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType qdt = qb.createQueryDefinition(ExcessReplicaTable.class);
      Query<ExcessReplicaTable> query = session.createQuery(qdt);
      List<ExcessReplicaTable> results = query.getResultList();
      if (results != null) {
        return results.size();
      } else {
        return 0;
      }
    } finally {
      afterTxCheck(true);
    }
  }

  public ExcessReplica findExcessReplicaByPK(String storageId, long blockId) throws TransactionContextException {
    beforeTxCheck();
    try {
      ExcessReplica searchInstance = new ExcessReplica(storageId, blockId);
      if (exReplicas.containsKey(searchInstance)) {
        return exReplicas.get(searchInstance);
      }

      if (removedExReplica.containsKey(searchInstance)) {
        return null;
      }

      Session session = DBConnector.obtainSession();
      Object[] keys = new Object[2];
      keys[0] = blockId;
      keys[1] = storageId;
      ExcessReplicaTable invTable = session.find(ExcessReplicaTable.class, keys);
      if (invTable == null) {
        return null;
      }

      ExcessReplica result = ReplicaFactory.createReplica(invTable);
      this.exReplicas.put(result, result);
      return result;
    } finally {
      afterTxCheck(true);
    }
  }

  public Collection<CorruptReplica> findAllCorruptBlocks() throws TransactionContextException {
    beforeTxCheck();
    try {
      if (allCorruptBlocksRead) {
        return corruptReplicas.values();
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicasTable> dobj = qb.createQueryDefinition(CorruptReplicasTable.class);
      Query<CorruptReplicasTable> query = session.createQuery(dobj);
      List<CorruptReplicasTable> ibts = query.getResultList();
      numCorruptBlocks = 0;
      syncCorruptReplicaInstances(ibts);

      allCorruptBlocksRead = true;

      return corruptReplicas.values();
    } finally {
      afterTxCheck(true);
    }
  }
  public Collection<DatanodeDescriptor> findCorruptReplica(long blockId) throws TransactionContextException {
    beforeTxCheck();
    try {
      Session session = DBConnector.obtainSession();
      
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicasTable> dobj = qb.createQueryDefinition(CorruptReplicasTable.class);
      Predicate pred = dobj.get("blockId").equal(dobj.param("blockId"));
      dobj.where(pred);
      Query<CorruptReplicasTable> query = session.createQuery(dobj);
      query.setParameter("blockId", blockId);
      
      List<CorruptReplicasTable> creplicas = query.getResultList();
      Collection<DatanodeDescriptor> datanodes = new TreeSet<DatanodeDescriptor>();
      
      for (CorruptReplicasTable c : creplicas) {
        // Fill the dnd data
        datanodes.add(DatanodeHelper.getDatanodeDescriptorByStorageId(c.getStorageId()));
        
      }
      syncCorruptReplicaInstances(creplicas);
      return datanodes;
      
    } finally {
      afterTxCheck(true);
    }
  }

  public DatanodeDescriptor findCorruptReplica(long blockId, String storageId) throws TransactionContextException {
    beforeTxCheck();
    try {
      CorruptReplica searchInstance = new CorruptReplica(blockId, storageId);
      if (corruptReplicas.containsKey(searchInstance)) {
        return DatanodeHelper.getDatanodeDescriptorByStorageId(corruptReplicas.get(searchInstance).getStorageId());
      }

      if (modifiedCorruptReplicas.containsKey(searchInstance)) {
        return DatanodeHelper.getDatanodeDescriptorByStorageId(modifiedCorruptReplicas.get(searchInstance).getStorageId());
      }

      // Not found in memory, search in database
      Session session = DBConnector.obtainSession();
      Object[] keys = new Object[2];
      keys[0] = blockId;
      keys[1] = storageId;
      CorruptReplicasTable corruptReplicaTable = session.find(CorruptReplicasTable.class, keys);
      if (corruptReplicaTable != null) {
        ReplicaFactory.createPersistable(searchInstance, corruptReplicaTable);
        corruptReplicas.put(searchInstance, searchInstance);
        return DatanodeHelper.getDatanodeDescriptorByStorageId(searchInstance.getStorageId());
      }
      
      throw new TransactionContextException("Corrupt replica for blockId: "+blockId+" and storageId: "+storageId+" does not exist");
    } finally {
      afterTxCheck(true);
    }
  }

  private Collection<CorruptReplica> syncCorruptReplicaInstances(List<CorruptReplicasTable> corruptReplicaRecords) {

    for (CorruptReplicasTable record : corruptReplicaRecords) {
      CorruptReplica corruptReplica = ReplicaFactory.createReplica(record);
      if (!removedCorruptReplicas.containsKey(corruptReplica)) {
        numCorruptBlocks++;
        if (!this.corruptReplicas.containsKey(corruptReplica)) {
          corruptReplicas.put(corruptReplica, corruptReplica);
        }
      }
    }

    return corruptReplicas.values();
  }

  public boolean containsUnderReplicatedBlock(long blockId) throws TransactionContextException {
    beforeTxCheck();
    try {

      // Not found in memory, search in database
      Session session = DBConnector.obtainSession();
      UnderReplicaBlocksTable urBlockTable = session.find(UnderReplicaBlocksTable.class, blockId);
      if (urBlockTable != null) {
        UnderReplicatedBlock urBlock = ReplicaFactory.createReplica(urBlockTable);
        urBlocks.put(urBlock, urBlock);
        return true;
      }
      // Not found
      return false;
    } finally {
      afterTxCheck(true);
    }
  }

  public List<UnderReplicatedBlock> findAllUnderReplicatedBlocks() throws TransactionContextException {
    beforeTxCheck();
    try {
      if (allUrBlocksRead) {
        return new ArrayList(urBlocks.values());
      }

      Session session = DBConnector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<UnderReplicaBlocksTable> dobj = qb.createQueryDefinition(UnderReplicaBlocksTable.class);
      Query<UnderReplicaBlocksTable> query = session.createQuery(dobj);
      List<UnderReplicaBlocksTable> ibts = query.getResultList();
      numUrBlocks = 0;
      syncUnderReplicatedBlockInstances(ibts);

      allCorruptBlocksRead = true;

      return new ArrayList(urBlocks.values());
    } finally {
      afterTxCheck(true);
    }
  }

  private Collection<UnderReplicatedBlock> syncUnderReplicatedBlockInstances(List<UnderReplicaBlocksTable> urBlockRecords) {

    for (UnderReplicaBlocksTable record : urBlockRecords) {
      UnderReplicatedBlock urBlock = ReplicaFactory.createReplica(record);
      if (!removedurBlocks.containsKey(urBlock)) {
        numUrBlocks++;
        if (!this.urBlocks.containsKey(urBlock)) {
          urBlocks.put(urBlock, urBlock);
        }
      }
    }

    return urBlocks.values();
  }

  public int countNonCorruptedUnderReplicatedBlocks(int level) throws TransactionContextException {
    beforeTxCheck();
    try {
      Session session = DBConnector.obtainSession();
      
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<UnderReplicaBlocksTable> dobj = qb.createQueryDefinition(UnderReplicaBlocksTable.class);
      Predicate pred = dobj.get("level").lessThan(dobj.param("level"));
      dobj.where(pred);
      Query<UnderReplicaBlocksTable> query = session.createQuery(dobj);
      query.setParameter("level", level);
      
      List<UnderReplicaBlocksTable> urNoncorruptedBlocks = query.getResultList();
      syncUnderReplicatedBlockInstances(urNoncorruptedBlocks);
      return urNoncorruptedBlocks.size();
      
    } finally {
      afterTxCheck(true);
    }
  }

  public int countCorruptedUnderReplicatedBlocks(int level) throws TransactionContextException {
    beforeTxCheck();
    try {
      Session session = DBConnector.obtainSession();
      
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<UnderReplicaBlocksTable> dobj = qb.createQueryDefinition(UnderReplicaBlocksTable.class);
      Predicate pred = dobj.get("level").equal(dobj.param("level"));
      dobj.where(pred);
      Query<UnderReplicaBlocksTable> query = session.createQuery(dobj);
      query.setParameter("level", level);
      
      List<UnderReplicaBlocksTable> urCorruptedBlocks = query.getResultList();
      syncUnderReplicatedBlockInstances(urCorruptedBlocks);
      return urCorruptedBlocks.size();
      
    } finally {
      afterTxCheck(true);
    }
  }
}
