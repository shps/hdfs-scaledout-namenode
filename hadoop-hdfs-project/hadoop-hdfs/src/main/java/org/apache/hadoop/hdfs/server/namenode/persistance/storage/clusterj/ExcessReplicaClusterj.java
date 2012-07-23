package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ExcessReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaClusterj extends ExcessReplicaDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ExcessReplicaDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long storageId);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    String getStorageId();

    void setStorageId(String storageId);
  }
  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public int countAll() throws StorageException {
    try {
      QueryBuilder qb = session.getQueryBuilder();

      QueryDomainType qdt = qb.createQueryDefinition(ExcessReplicaDTO.class);
      Query<ExcessReplicaDTO> query = session.createQuery(qdt);
      List<ExcessReplicaDTO> results = query.getResultList();
      return results.size();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<ExcessReplica> removed, Collection<ExcessReplica> newed, Collection<ExcessReplica> modified) throws StorageException {
    try {
      for (ExcessReplica exReplica : newed) {
        ExcessReplicaDTO newInstance = session.newInstance(ExcessReplicaDTO.class);
        createPersistable(exReplica, newInstance);
        session.savePersistent(newInstance);
      }

      for (ExcessReplica exReplica : removed) {
        Object[] pk = new Object[2];
        pk[0] = exReplica.getBlockId();
        pk[1] = exReplica.getStorageId();
        session.deletePersistent(ExcessReplicaDTO.class, pk);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<ExcessReplica> findExcessReplicaByStorageId(String storageId) throws StorageException {
    try {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<ExcessReplicaDTO> qdt = qb.createQueryDefinition(ExcessReplicaDTO.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<ExcessReplicaDTO> query = session.createQuery(qdt);
      query.setParameter("param", storageId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public ExcessReplica findByPkey(Object[] params) throws StorageException {
    try {
      ExcessReplicaDTO invTable = session.find(ExcessReplicaDTO.class, params);
      if (invTable == null) {
        return null;
      }
      ExcessReplica result = createReplica(invTable);
      return result;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<ExcessReplica> createList(List<ExcessReplicaDTO> list) {
    List<ExcessReplica> result = new ArrayList<ExcessReplica>();
    for (ExcessReplicaDTO item : list) {
      result.add(createReplica(item));
    }
    return result;
  }

  private ExcessReplica createReplica(ExcessReplicaDTO exReplicaTable) {
    return new ExcessReplica(exReplicaTable.getStorageId(), exReplicaTable.getBlockId());
  }

  private void createPersistable(ExcessReplica exReplica, ExcessReplicaDTO exReplicaTable) {
    exReplicaTable.setBlockId(exReplica.getBlockId());
    exReplicaTable.setStorageId(exReplica.getStorageId());
  }
}
