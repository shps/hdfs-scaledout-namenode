package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.ReplicaFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ExcessReplicaFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ExcessReplicaStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaClusterj extends ExcessReplicaStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void remove(ExcessReplica exReplica) throws TransactionContextException {
    if (exReplicas.remove(exReplica) == null) {
      throw new TransactionContextException("Unattached excess-replica passed to be removed");
    }

    modifiedExReplica.remove(exReplica);
    removedExReplica.put(exReplica, exReplica);
  }

  @Override
  public Collection<ExcessReplica> findList(Finder<ExcessReplica> finder, Object... params) {
    ExcessReplicaFinder eFinder = (ExcessReplicaFinder) finder;
    Collection<ExcessReplica> result = null;

    switch (eFinder) {
      case ByStorageId:
        String sId = (String) params[0];
        result = findExcessReplicaByStorageId(sId);
        break;
    }

    return result;
  }

  @Override
  public ExcessReplica find(Finder<ExcessReplica> finder, Object... params) {
    ExcessReplicaFinder eFinder = (ExcessReplicaFinder) finder;
    ExcessReplica result = null;

    switch (eFinder) {
      case ByPKey:
        result = findByPkey(params);
        break;
    }

    return result;
  }

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
  public void update(ExcessReplica exReplica) throws TransactionContextException {
    if (removedExReplica.containsKey(exReplica)) {
      throw new TransactionContextException("Removed excess-replica passed to be persisted");
    }

    exReplicas.put(exReplica, exReplica);
    modifiedExReplica.put(exReplica, exReplica);
  }

  @Override
  public void add(ExcessReplica entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void commit() {
    for (ExcessReplica exReplica : modifiedExReplica.values()) {
      ExcessReplicaTable newInstance = session.newInstance(ExcessReplicaTable.class);
      ReplicaFactory.createPersistable(exReplica, newInstance);
      session.savePersistent(newInstance);
    }

    for (ExcessReplica exReplica : removedExReplica.values()) {
      Object[] pk = new Object[2];
      pk[0] = exReplica.getBlockId();
      pk[1] = exReplica.getStorageId();
      session.deletePersistent(ExcessReplicaTable.class, pk);
    }
  }

  private Collection<ExcessReplica> findExcessReplicaByStorageId(String storageId) {
    if (!storageIdToExReplica.containsKey(storageId)) {
      {
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<ExcessReplicaTable> qdt = qb.createQueryDefinition(ExcessReplicaTable.class);
        qdt.where(qdt.get("storageId").equal(qdt.param("param")));
        Query<ExcessReplicaTable> query = session.createQuery(qdt);
        query.setParameter("param", storageId);
        List<ExcessReplicaTable> invBlockTables = query.getResultList();
        TreeSet<ExcessReplica> exReplicaSet = syncExcessReplicaInstances(invBlockTables);
        storageIdToExReplica.put(storageId, exReplicaSet);
      }
    }
    return storageIdToExReplica.get(storageId);
  }

  private TreeSet<ExcessReplica> syncExcessReplicaInstances(List<ExcessReplicaTable> exReplicaTables) {
    TreeSet<ExcessReplica> replicaSet = new TreeSet<ExcessReplica>();

    if (exReplicaTables != null) {
      for (ExcessReplicaTable ert : exReplicaTables) {
        ExcessReplica replica = ReplicaFactory.createReplica(ert);
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

  private ExcessReplica findByPkey(Object[] params) {
    long blockId = (Long) params[0];
    String storageId = (String) params[1];
    ExcessReplica searchInstance = new ExcessReplica(storageId, blockId);
    if (exReplicas.containsKey(searchInstance)) {
      return exReplicas.get(searchInstance);
    }

    if (removedExReplica.containsKey(searchInstance)) {
      return null;
    }

    ExcessReplicaTable invTable = session.find(ExcessReplicaTable.class, params);
    if (invTable == null) {
      return null;
    }

    ExcessReplica result = ReplicaFactory.createReplica(invTable);
    this.exReplicas.put(result, result);
    return result;
  }
}
