package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ExcessReplicaStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaClusterj extends ExcessReplicaStorage {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public int countAll() {
    QueryBuilder qb = session.getQueryBuilder();

    QueryDomainType qdt = qb.createQueryDefinition(ExcessReplicaTable.class);
    Query<ExcessReplicaTable> query = session.createQuery(qdt);
    List<ExcessReplicaTable> results = query.getResultList();
    if (results != null) {
      return results.size();
    } else {
      return 0;
    }
  }

  @Override
  public void commit() {
    for (ExcessReplica exReplica : newExReplica.values()) {
      ExcessReplicaTable newInstance = session.newInstance(ExcessReplicaTable.class);
      createPersistable(exReplica, newInstance);
      session.savePersistent(newInstance);
    }

    for (ExcessReplica exReplica : removedExReplica.values()) {
      Object[] pk = new Object[2];
      pk[0] = exReplica.getBlockId();
      pk[1] = exReplica.getStorageId();
      session.deletePersistent(ExcessReplicaTable.class, pk);
    }
  }

  @Override
  protected TreeSet<ExcessReplica> findExcessReplicaByStorageId(String storageId) {

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<ExcessReplicaTable> qdt = qb.createQueryDefinition(ExcessReplicaTable.class);
    qdt.where(qdt.get("storageId").equal(qdt.param("param")));
    Query<ExcessReplicaTable> query = session.createQuery(qdt);
    query.setParameter("param", storageId);
    List<ExcessReplicaTable> invBlockTables = query.getResultList();
    TreeSet<ExcessReplica> exReplicaSet = syncExcessReplicaInstances(invBlockTables);

    return exReplicaSet;
  }

  @Override
  protected ExcessReplica findByPkey(Object[] params) {
    ExcessReplicaTable invTable = session.find(ExcessReplicaTable.class, params);
    if (invTable == null) {
      return null;
    }
    ExcessReplica result = createReplica(invTable);
    return result;
  }

  private TreeSet<ExcessReplica> syncExcessReplicaInstances(List<ExcessReplicaTable> exReplicaTables) {
    TreeSet<ExcessReplica> replicaSet = new TreeSet<ExcessReplica>();

    if (exReplicaTables != null) {
      for (ExcessReplicaTable ert : exReplicaTables) {
        ExcessReplica replica = createReplica(ert);
        if (!removedExReplica.containsKey(replica)) {
          if (exReplicas.containsKey(replica)) {
            replicaSet.add(exReplicas.get(replica));
          } else {
            exReplicas.put(replica, replica);
            replicaSet.add(replica);
          }
        }
      }
    }

    return replicaSet;
  }

  private ExcessReplica createReplica(ExcessReplicaTable exReplicaTable) {
    return new ExcessReplica(exReplicaTable.getStorageId(), exReplicaTable.getBlockId());
  }

  private void createPersistable(ExcessReplica exReplica, ExcessReplicaTable exReplicaTable) {
    exReplicaTable.setBlockId(exReplica.getBlockId());
    exReplicaTable.setStorageId(exReplica.getStorageId());
  }
}
