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
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.PendingBlockStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockClusterj extends PendingBlockStorage {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public void commit() {
    for (PendingBlockInfo p : modifiedPendings.values()) {
      PendingBlockTable pTable = session.newInstance(PendingBlockTable.class);
      createPersistablePendingBlockInfo(p, pTable);
      session.savePersistent(pTable);
    }

    for (PendingBlockInfo p : removedPendings.values()) {
      PendingBlockTable pTable = session.newInstance(PendingBlockTable.class, p.getBlockId());
      session.deletePersistent(pTable);
    }
  }

  @Override
  protected PendingBlockInfo findByPKey(long blockId) {
    PendingBlockTable pendingTable = session.find(PendingBlockTable.class, blockId);
    PendingBlockInfo pendingBlock = null;
    if (pendingTable != null) {
      pendingBlock = createPendingBlockInfo(pendingTable);
    }

    return pendingBlock;
  }

  @Override
  protected List<PendingBlockInfo> findAll() {
    QueryBuilder qb = session.getQueryBuilder();
    Query<PendingBlockTable> query =
            session.createQuery(qb.createQueryDefinition(PendingBlockTable.class));
    List<PendingBlockTable> result = query.getResultList();
    syncPendingBlockInstances(result);
    return new ArrayList(pendings.values());
  }

  /**
   *
   * @param pendingTables
   * @return newly found pending blocks
   */
  private List<PendingBlockInfo> syncPendingBlockInstances(List<PendingBlockTable> pendingTables) {
    List<PendingBlockInfo> newPBlocks = new ArrayList<PendingBlockInfo>();
    for (PendingBlockTable pTable : pendingTables) {
      PendingBlockInfo p = createPendingBlockInfo(pTable);
      if (pendings.containsKey(p.getBlockId())) {
        newPBlocks.add(pendings.get(p.getBlockId()));
      } else if (!removedPendings.containsKey(p.getBlockId())) {
        pendings.put(p.getBlockId(), p);
        newPBlocks.add(p);
      }
    }

    return newPBlocks;
  }

  @Override
  protected List<PendingBlockInfo> findByTimeLimit(long timeLimit) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<PendingBlockTable> qdt = qb.createQueryDefinition(PendingBlockTable.class);
    PredicateOperand predicateOp = qdt.get("timestamp");
    String paramName = "timelimit";
    PredicateOperand param = qdt.param(paramName);
    Predicate lessThan = predicateOp.lessThan(param);
    qdt.where(lessThan);
    Query query = session.createQuery(qdt);
    query.setParameter(paramName, timeLimit);
    List<PendingBlockTable> result = query.getResultList();
    if (result != null) {
      return syncPendingBlockInstances(result);
    }

    return null;
  }

  private PendingBlockInfo createPendingBlockInfo(PendingBlockTable pendingTable) {
    return new PendingBlockInfo(pendingTable.getBlockId(),
            pendingTable.getTimestamp(), pendingTable.getNumReplicasInProgress());
  }

  private void createPersistablePendingBlockInfo(PendingBlockInfo pendingBlock, PendingBlockTable pendingTable) {
    pendingTable.setBlockId(pendingBlock.getBlockId());
    pendingTable.setNumReplicasInProgress(pendingBlock.getNumReplicas());
    pendingTable.setTimestamp(pendingBlock.getTimeStamp());
  }
}
