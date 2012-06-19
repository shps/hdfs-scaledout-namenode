package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.PendingBlockInfoFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.PendingBlockFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.PendingBlockStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockClusterj extends PendingBlockStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void remove(PendingBlockInfo pendingBlock) throws TransactionContextException {
    if (pendings.remove(pendingBlock.getBlockId()) == null) {
      throw new TransactionContextException("Unattached pending-block passed to be removed");
    }
    modifiedPendings.remove(pendingBlock.getBlockId());
    removedPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  @Override
  public List<PendingBlockInfo> findList(Finder<PendingBlockInfo> finder, Object... params) {
    PendingBlockFinder pFinder = (PendingBlockFinder) finder;
    List<PendingBlockInfo> result = null;
    switch (pFinder) {
      case ByTimeLimit:
        long timeLimit = (Long) params[0];
        result = findByTimeLimit(timeLimit);
        break;
      case All:
        result = findAll();
        break;
    }

    return result;
  }

  @Override
  public PendingBlockInfo find(Finder<PendingBlockInfo> finder, Object... params) {
    PendingBlockFinder pFinder = (PendingBlockFinder) finder;
    PendingBlockInfo result = null;
    switch (pFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        result = findByPKey(blockId);
        break;
    }

    return result;
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(PendingBlockInfo pendingBlock) throws TransactionContextException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    modifiedPendings.put(pendingBlock.getBlockId(), pendingBlock);
  }

  @Override
  public void add(PendingBlockInfo entity) throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commit() {
    for (PendingBlockInfo p : modifiedPendings.values()) {
      PendingReplicationBlockTable pTable = session.newInstance(PendingReplicationBlockTable.class);
      PendingBlockInfoFactory.createPersistablePendingBlockInfo(p, pTable);
      session.savePersistent(pTable);
    }

    for (PendingBlockInfo p : removedPendings.values()) {
      PendingReplicationBlockTable pTable = session.newInstance(PendingReplicationBlockTable.class, p.getBlockId());
      session.deletePersistent(pTable);
    }
  }

  private PendingBlockInfo findByPKey(long blockId) {
    if (this.pendings.containsKey(blockId)) {
      return this.pendings.get(blockId);
    }

    if (this.removedPendings.containsKey(blockId)) {
      return null;
    }

    PendingReplicationBlockTable pendingTable = session.find(PendingReplicationBlockTable.class, blockId);
    PendingBlockInfo pendingBlock = null;
    if (pendingTable != null) {
      pendingBlock = PendingBlockInfoFactory.createPendingBlockInfo(pendingTable);
      this.pendings.put(blockId, pendingBlock);
    }

    return pendingBlock;
  }

  private List<PendingBlockInfo> findAll() {
    if (allPendingRead) {
      return new ArrayList(pendings.values());
    }

    QueryBuilder qb = session.getQueryBuilder();
    Query<PendingReplicationBlockTable> query =
            session.createQuery(qb.createQueryDefinition(PendingReplicationBlockTable.class));
    List<PendingReplicationBlockTable> result = query.getResultList();
    syncPendingBlockInstances(result);
    return new ArrayList(pendings.values());
  }

  /**
   *
   * @param pendingTables
   * @return newly found pending blocks
   */
  private List<PendingBlockInfo> syncPendingBlockInstances(List<PendingReplicationBlockTable> pendingTables) {
    List<PendingBlockInfo> newPBlocks = new ArrayList<PendingBlockInfo>();
    for (PendingReplicationBlockTable pTable : pendingTables) {
      PendingBlockInfo p = PendingBlockInfoFactory.createPendingBlockInfo(pTable);
      if (pendings.containsKey(p.getBlockId())) {
        newPBlocks.add(pendings.get(p.getBlockId()));
      } else if (!removedPendings.containsKey(p.getBlockId())) {
        pendings.put(p.getBlockId(), p);
        newPBlocks.add(p);
      }
    }

    return newPBlocks;
  }

  private List<PendingBlockInfo> findByTimeLimit(long timeLimit) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<PendingReplicationBlockTable> qdt = qb.createQueryDefinition(PendingReplicationBlockTable.class);
    PredicateOperand predicateOp = qdt.get("timestamp");
    String paramName = "timelimit";
    PredicateOperand param = qdt.param(paramName);
    Predicate lessThan = predicateOp.lessThan(param);
    qdt.where(lessThan);
    Query query = session.createQuery(qdt);
    query.setParameter(paramName, timeLimit);
    List<PendingReplicationBlockTable> result = query.getResultList();
    if (result != null) {
      return syncPendingBlockInstances(result);
    }

    return null;
  }
}
