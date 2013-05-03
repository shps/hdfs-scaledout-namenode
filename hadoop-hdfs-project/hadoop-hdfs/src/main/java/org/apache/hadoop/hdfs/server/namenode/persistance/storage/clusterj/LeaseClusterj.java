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
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.mysqlserver.CountHelper;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseClusterj extends LeaseDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeaseDTO {

    @PrimaryKey
    @Column(name = HOLDER)
    String getHolder();

    void setHolder(String holder);

    @Column(name = LAST_UPDATE)
    long getLastUpdate();

    void setLastUpdate(long last_upd);

    @Column(name = HOLDER_ID)
    int getHolderId();

    void setHolderId(int holder_id);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public int countAll() throws StorageException {
    return CountHelper.countAll(TABLE_NAME);
  }

  @Override
  public Lease findByPKey(String holder) throws StorageException {
    try {
      Session session = connector.obtainSession();
      LeaseDTO lTable = session.find(LeaseDTO.class, holder);
      if (lTable != null) {
        Lease lease = createLease(lTable);
        return lease;
      }
      return null;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Lease findByHolderId(int holderId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeaseDTO> dobj = qb.createQueryDefinition(LeaseDTO.class);

      dobj.where(dobj.get("holderId").equal(dobj.param("param")));

      Query<LeaseDTO> query = session.createQuery(dobj);
      query.setParameter("param", holderId); //the WHERE clause of SQL
      List<LeaseDTO> leaseTables = query.getResultList();

      if (leaseTables.size() > 1) {
        LeaseManager.LOG.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
        return null;
      } else if (leaseTables.size() == 1) {
        Lease lease = createLease(leaseTables.get(0));
        return lease;
      } else {
        LeaseManager.LOG.info("No rows found for holderID:" + holderId + " in Lease table");
        return null;
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<Lease> findAll() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeaseDTO> dobj = qb.createQueryDefinition(LeaseDTO.class);
      Query<LeaseDTO> query = session.createQuery(dobj);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<Lease> findByTimeLimit(long timeLimit) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType dobj = qb.createQueryDefinition(LeaseDTO.class);
      PredicateOperand propertyPredicate = dobj.get("lastUpdate");
      String param = "timelimit";
      PredicateOperand propertyLimit = dobj.param(param);
      Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
      dobj.where(lessThan);
      Query query = session.createQuery(dobj);
      query.setParameter(param, new Long(timeLimit));
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<Lease> removed, Collection<Lease> newed, Collection<Lease> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (Lease l : newed) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        session.savePersistent(lTable);
      }

      for (Lease l : modified) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class);
        createPersistableLeaseInstance(l, lTable);
        session.savePersistent(lTable);
      }

      for (Lease l : removed) {
        LeaseDTO lTable = session.newInstance(LeaseDTO.class, l.getHolder());
        session.deletePersistent(lTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private SortedSet<Lease> createList(List<LeaseDTO> list) {
    SortedSet<Lease> finalSet = new TreeSet<Lease>();
    for (LeaseDTO dto : list) {
      finalSet.add(createLease(dto));
    }

    return finalSet;
  }

  private Lease createLease(LeaseDTO lTable) {
    return new Lease(lTable.getHolder(), lTable.getHolderId(), lTable.getLastUpdate());
  }

  private void createPersistableLeaseInstance(Lease lease, LeaseDTO lTable) {
    lTable.setHolder(lease.getHolder());
    lTable.setHolderId(lease.getHolderID());
    lTable.setLastUpdate(lease.getLastUpdated());
  }
}
