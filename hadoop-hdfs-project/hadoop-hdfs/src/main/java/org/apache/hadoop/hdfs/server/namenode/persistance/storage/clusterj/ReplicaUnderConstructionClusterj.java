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
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.ReplicaUnderConstructionContext;

@PersistenceCapable(table = ReplicaUnderConstructionContext.TABLE_NAME)
interface ReplicaUcTable {

  @PrimaryKey
  @Column(name = ReplicaUnderConstructionContext.BLOCK_ID)
  long getBlockId();

  void setBlockId(long blkid);

  @PrimaryKey
  @Column(name = ReplicaUnderConstructionContext.STORAGE_ID)
  @Index(name = "idx_datanodeStorage")
  String getStorageId();

  void setStorageId(String id);

  @Column(name = ReplicaUnderConstructionContext.REPLICA_INDEX)
  int getIndex();

  void setIndex(int index);

  @Column(name = ReplicaUnderConstructionContext.STATE)
  int getState();

  void setState(int state);
}

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionClusterj extends ReplicaUnderConstructionContext {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public void prepare() {
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

  @Override
  public void removeAll() throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
