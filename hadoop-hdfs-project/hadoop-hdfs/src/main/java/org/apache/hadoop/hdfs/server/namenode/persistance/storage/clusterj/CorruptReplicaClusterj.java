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
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.CorruptReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class CorruptReplicaClusterj implements CorruptReplicaDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface CorruptReplicaDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    String getStorageId();

    void setStorageId(String id);
  }
  Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public int countAll() throws StorageException{
    try {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
      Query<CorruptReplicaDTO> query = session.createQuery(dobj);
      return createCorruptReplicaList(query.getResultList()).size();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<CorruptReplica> removed, Collection<CorruptReplica> newed, Collection<CorruptReplica> modified) throws StorageException {
    for (CorruptReplica corruptReplica : removed) {
      Object[] pk = new Object[2];
      pk[0] = corruptReplica.getBlockId();
      pk[1] = corruptReplica.getStorageId();
      session.deletePersistent(CorruptReplicaDTO.class, pk);
    }

    for (CorruptReplica corruptReplica : newed) {
      CorruptReplicaDTO newInstance = session.newInstance(CorruptReplicaDTO.class);
      createPersistable(corruptReplica, newInstance);
      session.savePersistent(newInstance);
    }

    for (CorruptReplica corruptReplica : modified) {
      CorruptReplicaDTO newInstance = session.newInstance(CorruptReplicaDTO.class);
      createPersistable(corruptReplica, newInstance);
      session.savePersistent(newInstance);
    }
  }

  @Override
  public CorruptReplica findByPk(long blockId, String storageId) throws StorageException {
    try {
      Object[] keys = new Object[2];
      keys[0] = blockId;
      keys[1] = storageId;
      CorruptReplicaDTO corruptReplicaTable = session.find(CorruptReplicaDTO.class, keys);
      if (corruptReplicaTable != null) {
        return createReplica(corruptReplicaTable);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<CorruptReplica> findAll() throws StorageException {
    try {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
      Query<CorruptReplicaDTO> query = session.createQuery(dobj);
      List<CorruptReplicaDTO> ibts = query.getResultList();
      return createCorruptReplicaList(ibts);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<CorruptReplica> findByBlockId(long blockId) throws StorageException {
    try {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<CorruptReplicaDTO> dobj = qb.createQueryDefinition(CorruptReplicaDTO.class);
      Predicate pred = dobj.get("blockId").equal(dobj.param("blockId"));
      dobj.where(pred);
      Query<CorruptReplicaDTO> query = session.createQuery(dobj);
      query.setParameter("blockId", blockId);
      List<CorruptReplicaDTO> creplicas = query.getResultList();
      return createCorruptReplicaList(creplicas);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private CorruptReplica createReplica(CorruptReplicaDTO corruptReplicaTable) {
    return new CorruptReplica(corruptReplicaTable.getBlockId(), corruptReplicaTable.getStorageId());
  }

  private List<CorruptReplica> createCorruptReplicaList(List<CorruptReplicaDTO> persistables) {
    List<CorruptReplica> replicas = new ArrayList<CorruptReplica>();
    for (CorruptReplicaDTO bit : persistables) {
      replicas.add(createReplica(bit));
    }
    return replicas;
  }

  private void createPersistable(CorruptReplica corruptReplica, CorruptReplicaDTO corruptReplicaTable) {
    corruptReplicaTable.setBlockId(corruptReplica.getBlockId());
    corruptReplicaTable.setStorageId(corruptReplica.getStorageId());
  }

}
