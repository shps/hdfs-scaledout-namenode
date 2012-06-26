package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.ReplicaUnderConstructionStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionClusterj extends ReplicaUnderConstructionStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void commit() {
    for (ReplicaUnderConstruction replica : removedReplicasUc.values()) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(ReplicaUcTable.class, pk);
    }

    for (ReplicaUnderConstruction replica : newReplicasUc.values()) {
      ReplicaUcTable newInstance = session.newInstance(ReplicaUcTable.class);
      createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  @Override
  protected List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<ReplicaUcTable> dobj = qb.createQueryDefinition(ReplicaUcTable.class);
    dobj.where(dobj.get("blockId").equal(dobj.param("param")));
    Query<ReplicaUcTable> query = session.createQuery(dobj);
    query.setParameter("param", blockId);
    List<ReplicaUcTable> storedReplicas = query.getResultList();
    List<ReplicaUnderConstruction> replicas = createReplicaList(storedReplicas);

    return replicas;
  }
  
  private List<ReplicaUnderConstruction> createReplicaList(List<ReplicaUcTable> replicaUc) {
    List<ReplicaUnderConstruction> replicas = new ArrayList<ReplicaUnderConstruction>(replicaUc.size());
    for (ReplicaUcTable t : replicaUc) {
      replicas.add(new ReplicaUnderConstruction(HdfsServerConstants.ReplicaState.values()[t.getState()],
              t.getStorageId(), t.getBlockId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(ReplicaUnderConstruction replica, ReplicaUcTable newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setState(replica.getState().ordinal());
  }
}
