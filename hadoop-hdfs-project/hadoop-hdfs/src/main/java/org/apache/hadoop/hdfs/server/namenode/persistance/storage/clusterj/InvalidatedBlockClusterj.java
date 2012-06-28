package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.InvalidatedBlockStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockClusterj extends InvalidatedBlockStorage {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  protected List<InvalidatedBlock> findAllInvalidatedBlocks() {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksTable.class);
    List<InvalidateBlocksTable> ibts = session.createQuery(qdt).getResultList();
    syncInvalidatedBlockInstances(ibts);

    return new ArrayList<InvalidatedBlock>(invBlocks.values());
  }

  @Override
  public int countAll() {
    return findAllInvalidatedBlocks().size();
  }

  @Override
  public void commit() {
    for (InvalidatedBlock invBlock : newInvBlocks.values()) {
      InvalidateBlocksTable newInstance = session.newInstance(InvalidateBlocksTable.class);
      createPersistable(invBlock, newInstance);
      session.savePersistent(newInstance);
    }

    for (InvalidatedBlock invBlock : removedInvBlocks.values()) {
      Object[] pk = new Object[2];
      pk[0] = invBlock.getBlockId();
      pk[1] = invBlock.getStorageId();
      session.deletePersistent(InvalidateBlocksTable.class, pk);
    }
  }

  @Override
  protected List<InvalidatedBlock> findInvalidatedBlockByStorageId(String storageId) {
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
  }

  private void syncInvalidatedBlockInstances(List<InvalidateBlocksTable> invBlockTables) {
    for (InvalidateBlocksTable bTable : invBlockTables) {
      InvalidatedBlock invBlock = createReplica(bTable);
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
        } else {
          invBlocks.put(invBlock, invBlock);
        }
        if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
          storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
        } else {
          HashSet<InvalidatedBlock> invBlockList = new HashSet<InvalidatedBlock>();
          invBlockList.add(invBlock);
          storageIdToInvBlocks.put(invBlock.getStorageId(), invBlockList);
        }
      }
    }
  }

  @Override
  protected InvalidatedBlock findInvBlockByPkey(Object[] params) {
    InvalidateBlocksTable invTable = session.find(InvalidateBlocksTable.class, params);
    if (invTable == null) {
      return null;
    }
    return createReplica(invTable);
  }

  private InvalidatedBlock createReplica(InvalidateBlocksTable invBlockTable) {
    return new InvalidatedBlock(invBlockTable.getStorageId(), invBlockTable.getBlockId(),
            invBlockTable.getGenerationStamp(), invBlockTable.getNumBytes());
  }

  private void createPersistable(InvalidatedBlock invBlock, InvalidateBlocksTable newInvTable) {
    newInvTable.setBlockId(invBlock.getBlockId());
    newInvTable.setStorageId(invBlock.getStorageId());
    newInvTable.setGenerationStamp(invBlock.getGenerationStamp());
    newInvTable.setNumBytes(invBlock.getNumBytes());
  }
}
