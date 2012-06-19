package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.IndexedReplicaFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class IndexedReplicaClusterj extends IndexedReplicaStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void remove(IndexedReplica replica) throws TransactionContextException {
    modifiedReplicas.remove(replica.cacheKey());
    removedReplicas.put(replica.cacheKey(), replica);
  }

  @Override
  public List<IndexedReplica> findList(Finder<IndexedReplica> finder, Object... params) {
    IndexedReplicaFinder iFinder = (IndexedReplicaFinder) finder;
    List<IndexedReplica> result = null;

    switch (iFinder) {
      case ByBlockId:
        long id = (Long) params[0];
        result = findReplicasById(id);
        break;
    }

    return result;
  }

  @Override
  public IndexedReplica find(Finder<IndexedReplica> finder, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(IndexedReplica replica) throws TransactionContextException {
    if (removedReplicas.containsKey(replica.cacheKey())) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    modifiedReplicas.put(replica.cacheKey(), replica);
  }

  @Override
  public void add(IndexedReplica entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void commit() {
    for (IndexedReplica replica : removedReplicas.values()) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(TripletsTable.class, pk);
    }

    for (IndexedReplica replica : modifiedReplicas.values()) {
      TripletsTable newInstance = session.newInstance(TripletsTable.class);
      IndexedReplicaFactory.createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  private List<IndexedReplica> findReplicasById(long id) {
    if (blockReplicas.containsKey(id)) {
      return blockReplicas.get(id);
    } else {
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
  }
}