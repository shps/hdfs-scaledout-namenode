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
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaUnderConstruntionDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionClusterj extends ReplicaUnderConstruntionDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ReplicaUcDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blkid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    @Index(name = "idx_datanodeStorage")
    String getStorageId();

    void setStorageId(String id);

    @Column(name = REPLICA_INDEX)
    int getIndex();

    void setIndex(int index);

    @Column(name = STATE)
    int getState();

    void setState(int state);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public void prepare(Collection<ReplicaUnderConstruction> removed, Collection<ReplicaUnderConstruction> newed, Collection<ReplicaUnderConstruction> modified) throws StorageException {
    Session session = connector.obtainSession();
    for (ReplicaUnderConstruction replica : removed) {
      Object[] pk = new Object[2];
      pk[0] = replica.getBlockId();
      pk[1] = replica.getStorageId();
      session.deletePersistent(ReplicaUcDTO.class, pk);
    }

    for (ReplicaUnderConstruction replica : newed) {
      ReplicaUcDTO newInstance = session.newInstance(ReplicaUcDTO.class);
      createPersistable(replica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaUcDTO> dobj = qb.createQueryDefinition(ReplicaUcDTO.class);
      dobj.where(dobj.get("blockId").equal(dobj.param("param")));
      Query<ReplicaUcDTO> query = session.createQuery(dobj);
      query.setParameter("param", blockId);
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<ReplicaUnderConstruction> createReplicaList(List<ReplicaUcDTO> replicaUc) {
    Session session = connector.obtainSession();
    List<ReplicaUnderConstruction> replicas = new ArrayList<ReplicaUnderConstruction>(replicaUc.size());
    for (ReplicaUcDTO t : replicaUc) {
      replicas.add(new ReplicaUnderConstruction(HdfsServerConstants.ReplicaState.values()[t.getState()],
              t.getStorageId(), t.getBlockId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(ReplicaUnderConstruction replica, ReplicaUcDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setState(replica.getState().ordinal());
  }
}
