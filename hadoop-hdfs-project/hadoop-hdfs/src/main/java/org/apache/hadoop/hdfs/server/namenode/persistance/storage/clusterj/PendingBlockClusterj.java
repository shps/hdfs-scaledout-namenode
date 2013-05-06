package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.PendingBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.mysqlserver.CountHelper;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockClusterj extends PendingBlockDataAccess {

  @Override
  public int countValidPendingBlocks(long timeLimit) throws StorageException {
    return CountHelper.countWithCriterion(TABLE_NAME, String.format("%s>%d", TIME_STAMP, timeLimit));
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface PendingBlockDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blockId);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);

    @Column(name = NUM_REPLICAS_IN_PROGRESS)
    int getNumReplicasInProgress();

    void setNumReplicasInProgress(int numReplicasInProgress);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public void prepare(Collection<PendingBlockInfo> removed, Collection<PendingBlockInfo> newed, Collection<PendingBlockInfo> modified) throws StorageException {
    Session session = connector.obtainSession();
    for (PendingBlockInfo p : newed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistablePendingBlockInfo(p, pTable);
      session.savePersistent(pTable);
    }
    for (PendingBlockInfo p : modified) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistablePendingBlockInfo(p, pTable);
      session.savePersistent(pTable);
    }

    for (PendingBlockInfo p : removed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class, p.getBlockId());
      session.deletePersistent(pTable);
    }
  }

  @Override
  public PendingBlockInfo findByPKey(long blockId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      PendingBlockDTO pendingTable = session.find(PendingBlockDTO.class, blockId);
      PendingBlockInfo pendingBlock = null;
      if (pendingTable != null) {
        pendingBlock = createPendingBlockInfo(pendingTable);
      }

      return pendingBlock;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<PendingBlockInfo> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      Query<PendingBlockDTO> query =
              session.createQuery(qb.createQueryDefinition(PendingBlockDTO.class));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<PendingBlockInfo> findByTimeLimit(long timeLimit) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<PendingBlockDTO> qdt = qb.createQueryDefinition(PendingBlockDTO.class);
      PredicateOperand predicateOp = qdt.get("timestamp");
      String paramName = "timelimit";
      PredicateOperand param = qdt.param(paramName);
      Predicate lessThan = predicateOp.lessThan(param);
      qdt.where(lessThan);
      Query query = session.createQuery(qdt);
      query.setParameter(paramName, timeLimit);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<PendingBlockInfo> createList(Collection<PendingBlockDTO> dtos) {
    List<PendingBlockInfo> list = new ArrayList<PendingBlockInfo>();
    for (PendingBlockDTO dto : dtos) {
      list.add(createPendingBlockInfo(dto));
    }
    return list;
  }

  private PendingBlockInfo createPendingBlockInfo(PendingBlockDTO pendingTable) {
    return new PendingBlockInfo(pendingTable.getBlockId(),
            pendingTable.getTimestamp(), pendingTable.getNumReplicasInProgress());
  }

  private void createPersistablePendingBlockInfo(PendingBlockInfo pendingBlock, PendingBlockDTO pendingTable) {
    pendingTable.setBlockId(pendingBlock.getBlockId());
    pendingTable.setNumReplicasInProgress(pendingBlock.getNumReplicas());
    pendingTable.setTimestamp(pendingBlock.getTimeStamp());
  }
}
