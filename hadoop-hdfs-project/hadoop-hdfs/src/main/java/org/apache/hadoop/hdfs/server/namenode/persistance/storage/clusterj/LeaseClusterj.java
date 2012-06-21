package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.LeaseFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeaseStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseClusterj extends LeaseStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public int countAll() {
    return findAll().size();
  }

  @Override
  public void commit() {
    for (Lease l : modifiedLeases.values()) {
      LeaseTable lTable = session.newInstance(LeaseTable.class);
      LeaseFactory.createPersistableLeaseInstance(l, lTable);
      session.savePersistent(lTable);
    }

    for (Lease l : removedLeases.values()) {
      LeaseTable lTable = session.newInstance(LeaseTable.class, l.getHolder());
      session.deletePersistent(lTable);
    }

  }

  @Override
  protected Lease findByPKey(String holder) {
    LeaseTable lTable = session.find(LeaseTable.class, holder);
    if (lTable != null) {
      Lease lease = LeaseFactory.createLease(lTable);
      return lease;
    }
    return null;
  }

  @Override
  protected Lease findByHolderId(int holderId) {

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);

    dobj.where(dobj.get(HOLDER_ID).equal(dobj.param("param")));

    Query<LeaseTable> query = session.createQuery(dobj);
    query.setParameter("param", holderId); //the WHERE clause of SQL
    List<LeaseTable> leaseTables = query.getResultList();

    if (leaseTables.size() > 1) {
      LeaseManager.LOG.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
      return null;
    } else if (leaseTables.size() == 1) {
      Lease lease = LeaseFactory.createLease(leaseTables.get(0));
      return lease;
    } else {
      LeaseManager.LOG.info("No rows found for holderID:" + holderId + " in Lease table");
      return null;
    }
  }

  @Override
  protected Collection<Lease> findAll() {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);
    Query<LeaseTable> query = session.createQuery(dobj);
    List<LeaseTable> resultList = query.getResultList();
    SortedSet<Lease> leaseSet = syncLeaseInstances(resultList);
    return leaseSet;
  }

  @Override
  protected Collection<Lease> findByTimeLimit(long timeLimit) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType dobj = qb.createQueryDefinition(LeaseTable.class);
    PredicateOperand propertyPredicate = dobj.get(LAST_UPDATE);
    String param = "timelimit";
    PredicateOperand propertyLimit = dobj.param(param);
    Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
    dobj.where(lessThan);
    Query query = session.createQuery(dobj);
    query.setParameter(param, new Long(timeLimit));
    List<LeaseTable> resultset = query.getResultList();
    return syncLeaseInstances(resultset);
  }

  private SortedSet<Lease> syncLeaseInstances(List<LeaseTable> lTables) {
    SortedSet<Lease> lSet = new TreeSet<Lease>();
    if (lTables != null) {
      for (LeaseTable lt : lTables) {
        Lease lease = LeaseFactory.createLease(lt);
        if (!removedLeases.containsKey(lease)) {
          if (leases.containsKey(lease.getHolder())) {
            lSet.add(leases.get(lease.getHolder()));
          } else {
            lSet.add(lease);
            leases.put(lease.getHolder(), lease);
            idToLease.put(lease.getHolderID(), lease);
          }
        }
      }
    }

    return lSet;
  }
}
