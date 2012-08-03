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
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaClusterj extends ReplicaDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ReplicaDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    @Index(name = "idx_datanodeStorage")
    String getStorageId();

    void setStorageId(String id);

    @Column(name = REPLICA_INDEX)
    int getIndex();

    void setIndex(int index);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public List<IndexedReplica> findReplicasById(long id) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ReplicaDTO> dobj = qb.createQueryDefinition(ReplicaDTO.class);
      dobj.where(dobj.get("blockId").equal(dobj.param("param")));
      Query<ReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("param", id);
      return createReplicaList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<IndexedReplica> removed, Collection<IndexedReplica> newed, Collection<IndexedReplica> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (IndexedReplica replica : removed) {
        Object[] pk = new Object[2];
        pk[0] = replica.getBlockId();
        pk[1] = replica.getStorageId();
        session.deletePersistent(ReplicaDTO.class, pk);
      }

      for (IndexedReplica replica : newed) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        session.savePersistent(newInstance);
      }

      for (IndexedReplica replica : modified) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        session.savePersistent(newInstance);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<IndexedReplica> createReplicaList(List<ReplicaDTO> triplets) {
    List<IndexedReplica> replicas = new ArrayList<IndexedReplica>(triplets.size());
    for (ReplicaDTO t : triplets) {
      replicas.add(new IndexedReplica(t.getBlockId(), t.getStorageId(), t.getIndex()));
    }
    return replicas;
  }

  private void createPersistable(IndexedReplica replica, ReplicaDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
  }
}