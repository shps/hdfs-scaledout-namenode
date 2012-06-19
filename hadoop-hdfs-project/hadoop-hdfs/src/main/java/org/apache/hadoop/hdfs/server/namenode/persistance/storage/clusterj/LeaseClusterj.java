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
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.LeaseFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeaseFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeaseStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseClusterj extends LeaseStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void remove(Lease lease) throws TransactionContextException {
    if (leases.remove(lease.getHolder()) == null) {
      throw new TransactionContextException("Unattached lease passed to be removed");
    }
    idToLease.remove(lease.getHolderID());
    modifiedLeases.remove(lease);
    removedLeases.put(lease, lease);
  }

  @Override
  public Collection<Lease> findList(Finder<Lease> finder, Object... params) {
    LeaseFinder lFinder = (LeaseFinder) finder;
    Collection<Lease> result = null;
    switch (lFinder) {
      case ByTimeLimit:
        long timeLimit = (Long) params[0];
        result = findByTimeLimit(timeLimit);
        break;
      case All:
        result = findAll();
        break;
    }
    return result;
  }

  @Override
  public Lease find(Finder<Lease> finder, Object... params) {
    LeaseFinder lFinder = (LeaseFinder) finder;
    Lease result = null;
    switch (lFinder) {
      case ByPKey:
        String holder = (String) params[0];
        result = findByPKey(holder);
        break;
      case ByHolderId:
        int holderId = (Integer) params[0];
        result = findByHolderId(holderId);
        break;
    }

    return result;
  }

  @Override
  public int countAll() {
    return findAll().size();
  }

  @Override
  public void update(Lease lease) throws TransactionContextException {
    if (removedLeases.containsKey(lease)) {
      throw new TransactionContextException("Removed lease passed to be persisted");
    }

    modifiedLeases.put(lease, lease);
    leases.put(lease.getHolder(), lease);
    idToLease.put(lease.getHolderID(), lease);
  }

  @Override
  public void add(Lease entity) throws TransactionContextException {
    update(entity);
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

  private Lease findByPKey(String holder) {
    if (leases.containsKey(holder)) {
      return leases.get(holder);
    }

    LeaseTable lTable = session.find(LeaseTable.class, holder);
    if (lTable != null) {
      Lease lease = LeaseFactory.createLease(lTable);
      leases.put(lease.getHolder(), lease);
      return lease;
    }
    return null;
  }

  private Lease findByHolderId(int holderId) {
    if (idToLease.containsKey(holderId)) {
      return idToLease.get(holderId);
    }

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);

    dobj.where(dobj.get("holderID").equal(dobj.param("param")));

    Query<LeaseTable> query = session.createQuery(dobj);
    query.setParameter("param", holderId); //the WHERE clause of SQL
    List<LeaseTable> leaseTables = query.getResultList();

    if (leaseTables.size() > 1) {
      LeaseManager.LOG.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
      return null;
    } else if (leaseTables.size() == 1) {
      Lease lease = LeaseFactory.createLease(leaseTables.get(0));
      leases.put(lease.getHolder(), lease);
      idToLease.put(lease.getHolderID(), lease);
      return lease;
    } else {
      LeaseManager.LOG.info("No rows found for holderID:" + holderId + " in Lease table");
      return null;
    }
  }

  private Collection<Lease> findAll() {
    if (allLeasesRead) {
      return new TreeSet<Lease>(this.leases.values());
    }

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);
    Query<LeaseTable> query = session.createQuery(dobj);
    List<LeaseTable> resultList = query.getResultList();
    SortedSet<Lease> leaseSet = syncLeaseInstances(resultList);
    allLeasesRead = true;
    return leaseSet;
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

  private Collection<Lease> findByTimeLimit(long timeLimit) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType dobj = qb.createQueryDefinition(LeaseTable.class);
    PredicateOperand propertyPredicate = dobj.get("lastUpdate");
    String param = "timelimit";
    PredicateOperand propertyLimit = dobj.param(param);
    Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
    dobj.where(lessThan);
    Query query = session.createQuery(dobj);
    query.setParameter(param, new Long(timeLimit));
    List<LeaseTable> resultset = query.getResultList();
    return syncLeaseInstances(resultset);
  }
}
