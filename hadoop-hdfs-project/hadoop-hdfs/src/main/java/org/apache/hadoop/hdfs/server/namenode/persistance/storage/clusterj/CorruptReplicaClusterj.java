package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.CorruptReplicaContext;

@PersistenceCapable(table = "corrupt_replicas")
interface CorruptReplicaTable {

  @PrimaryKey
  @Column(name = "blockId")
  long getBlockId();

  void setBlockId(long bid);

  @PrimaryKey
  @Column(name = "storageId")
  String getStorageId();

  void setStorageId(String id);
}

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class CorruptReplicaClusterj extends CorruptReplicaContext {

  Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public void prepare() {
    for (CorruptReplica corruptReplica : removedCorruptReplicas.values()) {
      Object[] pk = new Object[2];
      pk[0] = corruptReplica.getBlockId();
      pk[1] = corruptReplica.getStorageId();
      session.deletePersistent(CorruptReplicaTable.class, pk);
    }

    for (CorruptReplica corruptReplica : modifiedCorruptReplicas.values()) {
      CorruptReplicaTable newInstance = session.newInstance(CorruptReplicaTable.class);
      createPersistable(corruptReplica, newInstance);
      session.savePersistent(newInstance);
    }

  }

  @Override
  protected CorruptReplica findByPk(long blockId, String storageId) {
    Object[] keys = new Object[2];
    keys[0] = blockId;
    keys[1] = storageId;
    CorruptReplicaTable corruptReplicaTable = session.find(CorruptReplicaTable.class, keys);
    if (corruptReplicaTable != null) {
      CorruptReplica replica = createReplica(corruptReplicaTable);
      corruptReplicas.put(blockId + storageId, replica);
      return replica;
    }
    return null;

  }

  @Override
  protected List<CorruptReplica> findAll() {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<CorruptReplicaTable> dobj = qb.createQueryDefinition(CorruptReplicaTable.class);
    Query<CorruptReplicaTable> query = session.createQuery(dobj);
    List<CorruptReplicaTable> ibts = query.getResultList();
    return createCorruptReplicaList(ibts);
  }

  @Override
  protected List<CorruptReplica> findByBlockId(long blockId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<CorruptReplicaTable> dobj = qb.createQueryDefinition(CorruptReplicaTable.class);
    Predicate pred = dobj.get("blockId").equal(dobj.param("blockId"));
    dobj.where(pred);
    Query<CorruptReplicaTable> query = session.createQuery(dobj);
    query.setParameter("blockId", blockId);
    List<CorruptReplicaTable> creplicas = query.getResultList();
    return createCorruptReplicaList(creplicas);
  }
  private CorruptReplica createReplica(CorruptReplicaTable corruptReplicaTable) {
    return new CorruptReplica(corruptReplicaTable.getBlockId(), corruptReplicaTable.getStorageId());
  }

  private List<CorruptReplica> createCorruptReplicaList(List<CorruptReplicaTable> persistables) {
    List<CorruptReplica> replicas = new ArrayList<CorruptReplica>();
    for (CorruptReplicaTable bit : persistables) {
      replicas.add(createReplica(bit));
    }
    return replicas;
  }

  private void createPersistable(CorruptReplica corruptReplica, CorruptReplicaTable corruptReplicaTable) {
    corruptReplicaTable.setBlockId(corruptReplica.getBlockId());
    corruptReplicaTable.setStorageId(corruptReplica.getStorageId());
  }

  @Override
  public void removeAll() throws TransactionContextException {
    
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
