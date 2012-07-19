package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoClusterj implements BlockInfoDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockInfoDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = BLOCK_INDEX)
    int getBlockIndex();

    void setBlockIndex(int idx);

    @Column(name = INODE_ID)
    @Index(name = "idx_inodeid")
    long getINodeId();

    void setINodeId(long iNodeID);

    @Column(name = NUM_BYTES)
    long getNumBytes();

    void setNumBytes(long numbytes);

    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();

    void setGenerationStamp(long genstamp);

    @Column(name = BLOCK_UNDER_CONSTRUCTION_STATE)
    int getBlockUCState();

    void setBlockUCState(int BlockUCState);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long ts);

    @Column(name = PRIMARY_NODE_INDEX)
    int getPrimaryNodeIndex();

    void setPrimaryNodeIndex(int replication);

    @Column(name = BLOCK_RECOVERY_ID)
    long getBlockRecoveryId();

    void setBlockRecoveryId(long recoveryId);
  }
  Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public int countAll() throws StorageException {
    return findAllBlocks().size();
  }

  @Override
  public void prepare(Collection<BlockInfo> removed, Collection<BlockInfo> news, Collection<BlockInfo> modified) throws StorageException {
    try {
      for (BlockInfo block : removed) {
        BlockInfoDTO bTable = session.newInstance(BlockInfoDTO.class, block.getBlockId());
        session.deletePersistent(bTable);
      }

      for (BlockInfo block : news) {
        BlockInfoDTO bTable = session.newInstance(BlockInfoDTO.class);
        createPersistable(block, bTable);
        session.savePersistent(bTable);
      }

      for (BlockInfo block : modified) {
        BlockInfoDTO bTable = session.newInstance(BlockInfoDTO.class);
        createPersistable(block, bTable);
        session.savePersistent(bTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public BlockInfo findById(long blockId) throws StorageException {
    try {
      BlockInfoDTO bit = session.find(BlockInfoDTO.class, blockId);
      if (bit == null) {
        return null;
      }
      return createBlockInfo(bit);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<BlockInfo> findByInodeId(long id) throws StorageException {
    try {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoDTO.class);
      dobj.where(dobj.get("iNodeId").equal(dobj.param("param")));
      Query<BlockInfoDTO> query = session.createQuery(dobj);
      query.setParameter("param", id);
      return createBlockInfoList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<BlockInfo> findAllBlocks() throws StorageException {
    try {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoDTO> dobj = qb.createQueryDefinition(BlockInfoDTO.class);
      Query<BlockInfoDTO> query = session.createQuery(dobj);
      return createBlockInfoList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<BlockInfo> findByStorageId(String storageId) throws StorageException {
    try {
      List<BlockInfo> ret = new ArrayList<BlockInfo>();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaClusterj.ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
      dobj.where(dobj.get("storageId").equal(dobj.param("param")));
      Query<ReplicaClusterj.ReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("param", storageId);
      List<ReplicaClusterj.ReplicaDTO> triplets = query.getResultList();

      for (ReplicaClusterj.ReplicaDTO t : triplets) {
        ret.add(findById(t.getBlockId()));
      }
      return ret;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<BlockInfo> createBlockInfoList(List<BlockInfoDTO> bitList) {
    List<BlockInfo> list = new ArrayList<BlockInfo>();

    for (BlockInfoDTO blockInfoDTO : bitList) {
      list.add(createBlockInfo(blockInfoDTO));
    }

    return list;
  }

  private BlockInfo createBlockInfo(BlockInfoDTO bit) {
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

  private void createPersistable(BlockInfo block, BlockInfoDTO persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setNumBytes(block.getNumBytes());
    persistable.setGenerationStamp(block.getGenerationStamp());
    persistable.setINodeId(block.getInodeId());
    persistable.setTimestamp(block.getTimestamp());
    persistable.setBlockIndex(block.getBlockIndex());
    persistable.setBlockUCState(block.getBlockUCState().ordinal());
    if (block instanceof BlockInfoUnderConstruction) {
      BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) block;
      persistable.setPrimaryNodeIndex(ucBlock.getPrimaryNodeIndex());
      persistable.setBlockRecoveryId(ucBlock.getBlockRecoveryId());
    }
  }
}
