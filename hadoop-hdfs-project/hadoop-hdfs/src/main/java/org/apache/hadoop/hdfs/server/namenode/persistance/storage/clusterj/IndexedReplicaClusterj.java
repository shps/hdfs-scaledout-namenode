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
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.IndexedReplicaContext;


@PersistenceCapable(table = IndexedReplicaContext.TABLE_NAME)
interface IndexedReplicaTable {

  @PrimaryKey
  @Column(name = IndexedReplicaContext.BLOCK_ID)
  long getBlockId();

  void setBlockId(long bid);

  @PrimaryKey
  @Column(name = IndexedReplicaContext.STORAGE_ID)
  @Index(name = "idx_datanodeStorage")
  String getStorageId();

  void setStorageId(String id);

  @Column(name = IndexedReplicaContext.REPLICA_INDEX)
  int getIndex();

  void setIndex(int index);
}
/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class IndexedReplicaClusterj extends IndexedReplicaContext {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public void prepare() {
    for (IndexedReplica replica : removedReplicas.values()) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(IndexedReplicaTable.class, pk);
    }

    for (IndexedReplica replica : modifiedReplicas.values()) {
      IndexedReplicaTable newInstance = session.newInstance(IndexedReplicaTable.class);
      createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  @Override
  protected List<IndexedReplica> findReplicasById(long id) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<IndexedReplicaTable> dobj = qb.createQueryDefinition(IndexedReplicaTable.class);
    dobj.where(dobj.get("blockId").equal(dobj.param("param")));
    Query<IndexedReplicaTable> query = session.createQuery(dobj);
    query.setParameter("param", id);
    List<IndexedReplicaTable> triplets = query.getResultList();
    List<IndexedReplica> replicas = createReplicaList(triplets);

    return replicas;
  }
  
  private List<IndexedReplica> createReplicaList(List<IndexedReplicaTable> triplets) {
    List<IndexedReplica> replicas = new ArrayList<IndexedReplica>(triplets.size());
    for (IndexedReplicaTable t : triplets) {
      replicas.add(new IndexedReplica(t.getBlockId(), t.getStorageId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(IndexedReplica replica, IndexedReplicaTable newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
  }

  @Override
  public void removeAll() throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}