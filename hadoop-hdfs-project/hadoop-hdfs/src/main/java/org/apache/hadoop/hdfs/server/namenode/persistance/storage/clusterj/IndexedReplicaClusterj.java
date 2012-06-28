package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class IndexedReplicaClusterj extends IndexedReplicaStorage {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

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
      createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  @Override
  protected List<IndexedReplica> findReplicasById(long id) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
    dobj.where(dobj.get("blockId").equal(dobj.param("param")));
    Query<TripletsTable> query = session.createQuery(dobj);
    query.setParameter("param", id);
    List<TripletsTable> triplets = query.getResultList();
    List<IndexedReplica> replicas = createReplicaList(triplets);

    return replicas;
  }
  
  private List<IndexedReplica> createReplicaList(List<TripletsTable> triplets) {
    List<IndexedReplica> replicas = new ArrayList<IndexedReplica>(triplets.size());
    for (TripletsTable t : triplets) {
      replicas.add(new IndexedReplica(t.getBlockId(), t.getStorageId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(IndexedReplica replica, TripletsTable newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
  }
}