package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.ReplicaFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.InvalidatedBlockFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.InvalidatedBlockStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockClusterj extends InvalidatedBlockStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void remove(InvalidatedBlock invBlock) throws TransactionContextException {
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
  }

  @Override
  public List<InvalidatedBlock> findList(Finder<InvalidatedBlock> finder, Object... params) {
    InvalidatedBlockFinder iFinder = (InvalidatedBlockFinder) finder;
    List<InvalidatedBlock> result = null;

    switch (iFinder) {
      case ByStorageId:
        String storageId = (String) params[0];
        result = findInvalidatedBlockByStorageId(storageId);
        break;
      case All:
        result = findAllInvalidatedBlocks();
        break;
    }

    return result;
  }

  private List<InvalidatedBlock> findAllInvalidatedBlocks() {
    if (!allInvBlocksRead) {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksTable.class);
      List<InvalidateBlocksTable> ibts = session.createQuery(qdt).getResultList();
      syncInvalidatedBlockInstances(ibts);

      allInvBlocksRead = true;
    }

    return new ArrayList<InvalidatedBlock>(invBlocks.values());
  }

  @Override
  public InvalidatedBlock find(Finder<InvalidatedBlock> finder, Object... params) {
    InvalidatedBlockFinder iFinder = (InvalidatedBlockFinder) finder;
    InvalidatedBlock result = null;

    switch (iFinder) {
      case ByPrimaryKey:
        result = findInvBlockByPkey(params);
        break;
    }
    return result;
  }

  @Override
  public int countAll() {
    return findAllInvalidatedBlocks().size();
  }

  @Override
  public void update(InvalidatedBlock invBlock) throws TransactionContextException {
    if (removedInvBlocks.containsKey(invBlock)) {
      throw new TransactionContextException("Removed invalidated-block passed to be persisted");
    }

    addStorageToInvalidatedBlock(invBlock);

    invBlocks.put(invBlock, invBlock);
    modifiedInvBlocks.put(invBlock, invBlock);
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

  @Override
  public void add(InvalidatedBlock entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void commit() {
    for (InvalidatedBlock invBlock : modifiedInvBlocks.values()) {
      InvalidateBlocksTable newInstance = session.newInstance(InvalidateBlocksTable.class);
      ReplicaFactory.createPersistable(invBlock, newInstance);
      session.savePersistent(newInstance);
    }

    for (InvalidatedBlock invBlock : removedInvBlocks.values()) {
      Object[] pk = new Object[2];
      pk[0] = invBlock.getBlockId();
      pk[1] = invBlock.getStorageId();
      session.deletePersistent(InvalidateBlocksTable.class, pk);
    }
  }

  private List<InvalidatedBlock> findInvalidatedBlockByStorageId(String storageId) {

    if (storageIdToInvBlocks.containsKey(storageId)) {
      return new ArrayList<InvalidatedBlock>(this.storageIdToInvBlocks.get(storageId)); //clone the list reference
    }
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

  private InvalidatedBlock findInvBlockByPkey(Object[] params) {
    long blockId = (Long) params[0];
    String storageId = (String) params[1];

    InvalidatedBlock searchInstance = new InvalidatedBlock(storageId, blockId);
    if (invBlocks.containsKey(searchInstance)) {
      return invBlocks.get(searchInstance);
    }

    if (removedInvBlocks.containsKey(searchInstance)) {
      return null;
    }

    InvalidateBlocksTable invTable = session.find(InvalidateBlocksTable.class, params);
    if (invTable == null) {
      return null;
    }

    InvalidatedBlock result = ReplicaFactory.createReplica(invTable);
    this.invBlocks.put(result, result);
    return result;
  }
}
