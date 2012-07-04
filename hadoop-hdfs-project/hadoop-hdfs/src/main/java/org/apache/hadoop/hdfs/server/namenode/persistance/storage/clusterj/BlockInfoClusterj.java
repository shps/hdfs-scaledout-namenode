package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.BlockInfoContext;

@PersistenceCapable(table = BlockInfoContext.TABLE_NAME)
interface BlockInfoTable {

  @PrimaryKey
  @Column(name = BlockInfoContext.BLOCK_ID)
  long getBlockId();

  void setBlockId(long bid);

  @Column(name = BlockInfoContext.BLOCK_INDEX)
  int getBlockIndex();

  void setBlockIndex(int idx);

  @Column(name = BlockInfoContext.INODE_ID)
  @Index(name = "idx_inodeid")
  long getINodeId();

  void setINodeId(long iNodeID);

  @Column(name = BlockInfoContext.NUM_BYTES)
  long getNumBytes();

  void setNumBytes(long numbytes);

  @Column(name = BlockInfoContext.GENERATION_STAMP)
  long getGenerationStamp();

  void setGenerationStamp(long genstamp);

  @Column(name = BlockInfoContext.BLOCK_UNDER_CONSTRUCTION_STATE)
  int getBlockUCState();

  void setBlockUCState(int BlockUCState);

  @Column(name = BlockInfoContext.TIME_STAMP)
  long getTimestamp();

  void setTimestamp(long ts);

  @Column(name = BlockInfoContext.PRIMARY_NODE_INDEX)
  int getPrimaryNodeIndex();

  void setPrimaryNodeIndex(int replication);

  @Column(name = BlockInfoContext.BLOCK_RECOVERY_ID)
  long getBlockRecoveryId();

  void setBlockRecoveryId(long recoveryId);
}

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoClusterj extends BlockInfoContext {

  Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  protected BlockInfo findById(long blockId) {
    BlockInfoTable bit = session.find(BlockInfoTable.class, blockId);
    BlockInfo block = null;
    if (bit == null) {
      return null;
    }
    try {
      block = createBlockInfo(bit);
    } catch (IOException ex) {
      Logger.getLogger(BlockInfoClusterj.class.getName()).log(Level.SEVERE, null, ex);
    }

    return block;
  }

  @Override
  protected List<BlockInfo> findByInodeId(long id) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
    dobj.where(dobj.get("iNodeId").equal(dobj.param("param")));
    Query<BlockInfoTable> query = session.createQuery(dobj);
    query.setParameter("param", id);
    List<BlockInfoTable> resultList = query.getResultList();
    List<BlockInfo> syncedList = null;
    try {
      syncedList = syncBlockInfoInstances(resultList);
    } catch (IOException ex) {
      Logger.getLogger(BlockInfoClusterj.class.getName()).log(Level.SEVERE, null, ex);
    }
    return syncedList;
  }

  @Override
  protected List<BlockInfo> findAllBlocks() {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
    Query<BlockInfoTable> query = session.createQuery(dobj);
    List<BlockInfoTable> resultList = query.getResultList();
    List<BlockInfo> syncedList = null;
    try {
      syncedList = syncBlockInfoInstances(resultList);
    } catch (IOException ex) {
      Logger.getLogger(BlockInfoClusterj.class.getName()).log(Level.SEVERE, null, ex);
    }
    return syncedList;
  }

  @Override
  protected List<BlockInfo> findByStorageId(String storageId) {
    List<BlockInfo> ret = new ArrayList<BlockInfo>();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<IndexedReplicaTable> dobj = qb.createQueryDefinition(IndexedReplicaTable.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("param")));
    Query<IndexedReplicaTable> query = session.createQuery(dobj);
    query.setParameter("param", storageId);
    List<IndexedReplicaTable> triplets = query.getResultList();

    for (IndexedReplicaTable t : triplets) {
      ret.add(findById(t.getBlockId()));
    }
    return ret;
  }

  @Override
  public void prepare() {
    for (BlockInfo block : removedBlocks.values()) {
      BlockInfoTable bTable = session.newInstance(BlockInfoTable.class, block.getBlockId());
      session.deletePersistent(bTable);
    }

    for (BlockInfo block : modifiedBlocks.values()) {
      BlockInfoTable bTable = session.newInstance(BlockInfoTable.class);
      createPersistable(block, bTable);
      session.savePersistent(bTable);
    }
  }

  @Override
  public int count(CounterType<BlockInfo> counter, Object... params) {
    BlockInfo.Counter bCounter = (BlockInfo.Counter) counter;
    switch (bCounter) {
      case All:
        findAllBlocks();
        return blocks.size();
    }
    return -1;
  }

  private List<BlockInfo> syncBlockInfoInstances(List<BlockInfoTable> newBlocks) throws IOException {
    List<BlockInfo> finalList = new ArrayList<BlockInfo>();

    for (BlockInfoTable blockTable : newBlocks) {
      BlockInfo blockInfo = createBlockInfo(blockTable);
      if (blocks.containsKey(blockInfo.getBlockId()) && !removedBlocks.containsKey(blockInfo.getBlockId())) {
        finalList.add(blocks.get(blockInfo.getBlockId()));
      } else {
        blocks.put(blockInfo.getBlockId(), blockInfo);
        finalList.add(blockInfo);
      }
    }

    return finalList;
  }

  private BlockInfo createBlockInfo(BlockInfoTable bit) throws IOException {
    Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());
    BlockInfo blockInfo = null;

    if (bit.getBlockUCState() > 0) { //UNDER_CONSTRUCTION, UNDER_RECOVERY, COMMITED
      blockInfo = new BlockInfoUnderConstruction(b);
      ((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.values()[bit.getBlockUCState()]);
      ((BlockInfoUnderConstruction) blockInfo).setPrimaryNodeIndex(bit.getPrimaryNodeIndex());
      ((BlockInfoUnderConstruction) blockInfo).setBlockRecoveryId(bit.getBlockRecoveryId());
    } else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE.ordinal()) {
      blockInfo = new BlockInfo(b);
    }

    blockInfo.setINodeId(bit.getINodeId());
    blockInfo.setTimestamp(bit.getTimestamp());
    blockInfo.setBlockIndex(bit.getBlockIndex());

    return blockInfo;
  }

  private void createPersistable(BlockInfo block, BlockInfoTable persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setNumBytes(block.getNumBytes());
    persistable.setGenerationStamp(block.getGenerationStamp());
    persistable.setINodeId(block.getINode().getId());
    persistable.setTimestamp(block.getTimestamp());
    persistable.setBlockIndex(block.getBlockIndex());
    persistable.setBlockUCState(block.getBlockUCState().ordinal());
    if (block instanceof BlockInfoUnderConstruction) {
      BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
      persistable.setPrimaryNodeIndex(ucBlock.getPrimaryNodeIndex());
      persistable.setBlockRecoveryId(ucBlock.getBlockRecoveryId());
    }
  }

  @Override
  public void removeAll() throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
