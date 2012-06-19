package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.BlockInfoStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoClusterj extends BlockInfoStorage {

  Session session = DBConnector.obtainSession();

  @Override
  public void remove(BlockInfo block) throws TransactionContextException {
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

  @Override
  public List<BlockInfo> findList(Finder<BlockInfo> finder, Object... params) {
    BlockInfoFinder bFinder = (BlockInfoFinder) finder;
    List<BlockInfo> result = null;
    switch (bFinder) {
      case ByInodeId:
        long inodeId = (Long) params[0];
        result = findByInodeId(inodeId);
        break;
      case ByStorageId:
        String storageId = (String) params[0];
        result = findByStorageId(storageId);
        break;
      case All:
        result = findAllBlocks();
    }

    return result;
  }

  @Override
  public BlockInfo find(Finder<BlockInfo> finder, Object... params) {
    BlockInfoFinder bFinder = (BlockInfoFinder) finder;
    BlockInfo result = null;
    switch (bFinder) {
      case ById:
        long id = (Long) params[0];
        result = findById(id);
        break;
    }

    return result;
  }

  private BlockInfo findById(long blockId) {
    BlockInfo block = blocks.get(blockId);
    if (block == null) {
      BlockInfoTable bit = session.find(BlockInfoTable.class, blockId);
      if (bit == null) {
        return null;
      }
      try {
        block = BlockInfoFactory.createBlockInfo(bit);
      } catch (IOException ex) {
        Logger.getLogger(BlockInfoClusterj.class.getName()).log(Level.SEVERE, null, ex);
      }
      blocks.put(blockId, block);
    }
    return block;
  }

  private List<BlockInfo> findByInodeId(long id) {
    if (inodeBlocks.containsKey(id)) {
      return inodeBlocks.get(id);
    } else {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
      dobj.where(dobj.get("iNodeID").equal(dobj.param("param")));
      Query<BlockInfoTable> query = session.createQuery(dobj);
      query.setParameter("param", id);
      List<BlockInfoTable> resultList = query.getResultList();
      List<BlockInfo> syncedList = null;
      try {
        syncedList = syncBlockInfoInstances(BlockInfoFactory.createBlockInfoList(resultList));
        inodeBlocks.put(id, syncedList);
      } catch (IOException ex) {
        Logger.getLogger(BlockInfoClusterj.class.getName()).log(Level.SEVERE, null, ex);
      }
      return syncedList;
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

  private List<BlockInfo> findAllBlocks() {
    if (allBlocksRead) {
      return new ArrayList<BlockInfo>(blocks.values());
    } else {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
      Query<BlockInfoTable> query = session.createQuery(dobj);
      List<BlockInfoTable> resultList = query.getResultList();
      List<BlockInfo> syncedList = null;
      try {
        syncedList = syncBlockInfoInstances(BlockInfoFactory.createBlockInfoList(resultList));
        allBlocksRead = true;
      } catch (IOException ex) {
        Logger.getLogger(BlockInfoClusterj.class.getName()).log(Level.SEVERE, null, ex);
      }
      return syncedList;
    }
  }

  private List<BlockInfo> findByStorageId(String storageId) {
    List<BlockInfo> ret = new ArrayList<BlockInfo>();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("param")));
    Query<TripletsTable> query = session.createQuery(dobj);
    query.setParameter("param", storageId);
    List<TripletsTable> triplets = query.getResultList();

    for (TripletsTable t : triplets) {
      ret.add(findById(t.getBlockId()));
    }
    return ret;
  }

  @Override
  public int countAll() {
    findAllBlocks();
    return blocks.size();
  }

  @Override
  public void update(BlockInfo block) throws TransactionContextException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    modifiedBlocks.put(block.getBlockId(), block);
  }

  @Override
  public void add(BlockInfo block) throws TransactionContextException {
    update(block);
  }

  @Override
  public void commit() {
    for (BlockInfo block : removedBlocks.values()) {
      BlockInfoTable bTable = session.newInstance(BlockInfoTable.class, block.getBlockId());
      session.deletePersistent(bTable);
    }

    for (BlockInfo block : modifiedBlocks.values()) {
      BlockInfoTable bTable = session.newInstance(BlockInfoTable.class);
      BlockInfoFactory.createPersistable(block, bTable);
      session.savePersistent(bTable);
    }
  }
}
