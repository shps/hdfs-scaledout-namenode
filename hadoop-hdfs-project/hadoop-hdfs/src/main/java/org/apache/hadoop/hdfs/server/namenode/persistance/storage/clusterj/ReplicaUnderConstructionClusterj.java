package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.ReplicaUcFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ReplicaUnderConstructionFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ReplicaUnderConstructionStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionClusterj extends ReplicaUnderConstructionStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void remove(ReplicaUnderConstruction replica) throws TransactionContextException {
    modifiedReplicasUc.remove(replica.cacheKey());
    removedReplicasUc.put(replica.cacheKey(), replica);
  }

  @Override
  public List<ReplicaUnderConstruction> findList(Finder<ReplicaUnderConstruction> finder, Object... params) {

    ReplicaUnderConstructionFinder rFinder = (ReplicaUnderConstructionFinder) finder;
    List<ReplicaUnderConstruction> result = null;
    switch (rFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        result = findReplicaUnderConstructionByBlockId(blockId);
        break;
    }

    return result;
  }

  @Override
  public ReplicaUnderConstruction find(Finder<ReplicaUnderConstruction> finder, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(ReplicaUnderConstruction replica) throws TransactionContextException {
    if (removedReplicasUc.containsKey(replica.cacheKey())) {
      throw new TransactionContextException("Removed  under constructionreplica passed to be persisted");
    }

    modifiedReplicasUc.put(replica.cacheKey(), replica);
  }

  @Override
  public void add(ReplicaUnderConstruction entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void commit() {
    for (ReplicaUnderConstruction replica : removedReplicasUc.values()) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(ReplicaUcTable.class, pk);
    }

    for (ReplicaUnderConstruction replica : modifiedReplicasUc.values()) {
      ReplicaUcTable newInstance = session.newInstance(ReplicaUcTable.class);
      ReplicaUcFactory.createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  private List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId) {

    if (blockReplicasUc.containsKey(blockId)) {
      return blockReplicasUc.get(blockId);
    } else {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaUcTable> dobj = qb.createQueryDefinition(ReplicaUcTable.class);
      dobj.where(dobj.get("blockId").equal(dobj.param("param")));
      Query<ReplicaUcTable> query = session.createQuery(dobj);
      query.setParameter("param", blockId);
      List<ReplicaUcTable> storedReplicas = query.getResultList();
      List<ReplicaUnderConstruction> replicas = ReplicaUcFactory.createReplicaList(storedReplicas);
      blockReplicasUc.put(blockId, replicas);
      return replicas;
    }
  }
}
