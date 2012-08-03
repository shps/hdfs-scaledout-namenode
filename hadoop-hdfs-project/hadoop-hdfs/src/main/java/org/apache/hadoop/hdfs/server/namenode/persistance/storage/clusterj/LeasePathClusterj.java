package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeasePathDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathClusterj extends LeasePathDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeasePathsDTO {

    @Column(name = HOLDER_ID)
    int getHolderId();

    void setHolderId(int holder_id);

    @PrimaryKey
    @Column(name = PATH)
    String getPath();

    void setPath(String path);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public void prepare(Collection<LeasePath> removed, Collection<LeasePath> newed, Collection<LeasePath> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (LeasePath lp : newed) {
        LeasePathsDTO lTable = session.newInstance(LeasePathsDTO.class);
        createPersistableLeasePathInstance(lp, lTable);
        session.savePersistent(lTable);
      }

      for (LeasePath lp : modified) {
        LeasePathsDTO lTable = session.newInstance(LeasePathsDTO.class);
        createPersistableLeasePathInstance(lp, lTable);
        session.savePersistent(lTable);
      }

      for (LeasePath lp : removed) {
        LeasePathsDTO lTable = session.newInstance(LeasePathsDTO.class, lp.getPath());
        session.deletePersistent(lTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<LeasePath> findByHolderId(int holderId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeasePathsDTO> dobj = qb.createQueryDefinition(LeasePathsDTO.class);
      dobj.where(dobj.get("holderId").equal(dobj.param("param")));
      Query<LeasePathsDTO> query = session.createQuery(dobj);
      query.setParameter("param", holderId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public LeasePath findByPKey(String path) throws StorageException {
    try {
      Session session = connector.obtainSession();
      LeasePathsDTO lPTable = session.find(LeasePathsDTO.class, path);
      LeasePath lPath = null;
      if (lPTable != null) {
        lPath = createLeasePath(lPTable);
      }
      return lPath;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<LeasePath> findByPrefix(String prefix) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
      PredicateOperand propertyPredicate = dobj.get("path");
      String param = "prefix";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate like = propertyPredicate.like(propertyLimit);
      dobj.where(like);
      Query query = session.createQuery(dobj);
      query.setParameter(param, prefix + "%");
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<LeasePath> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
      Query query = session.createQuery(dobj);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<LeasePath> createList(Collection<LeasePathsDTO> dtos) {
    List<LeasePath> list = new ArrayList<LeasePath>();
    for (LeasePathsDTO leasePathsDTO : dtos) {
      list.add(createLeasePath(leasePathsDTO));
    }
    return list;
  }

  private LeasePath createLeasePath(LeasePathsDTO leasePathTable) {
    return new LeasePath(leasePathTable.getPath(), leasePathTable.getHolderId());
  }

  private void createPersistableLeasePathInstance(LeasePath lp, LeasePathsDTO lTable) {
    lTable.setHolderId(lp.getHolderId());
    lTable.setPath(lp.getPath());
  }
}
