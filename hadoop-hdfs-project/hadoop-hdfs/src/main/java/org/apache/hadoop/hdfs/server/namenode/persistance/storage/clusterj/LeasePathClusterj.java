package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.LeasePathFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeasePathFinder;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeasePathStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathClusterj extends LeasePathStorage {

  private Session session = DBConnector.obtainSession();

  @Override
  public void remove(LeasePath lPath) throws TransactionContextException {
    if (leasePaths.remove(lPath) == null) {
      throw new TransactionContextException("Unattached lease-path passed to be removed");
    }

    pathToLeasePath.remove(lPath.getPath());
    modifiedLPaths.remove(lPath);
    if (holderLeasePaths.containsKey(lPath.getHolderId())) {
      Set<LeasePath> lSet = holderLeasePaths.get(lPath.getHolderId());
      lSet.remove(lPath);
      if (lSet.isEmpty()) {
        holderLeasePaths.remove(lPath.getHolderId());
      }
    }
    removedLPaths.put(lPath, lPath);
  }

  @Override
  public Collection<LeasePath> findList(Finder<LeasePath> finder, Object... params) {
    LeasePathFinder lFinder = (LeasePathFinder) finder;
    Collection<LeasePath> result = null;

    switch (lFinder) {
      case ByHolderId:
        int holderId = (Integer) params[0];
        result = findByHolderId(holderId);
        break;
      case ByPrefix:
        String prefix = (String) params[0];
        result = findByPrefix(prefix);
        break;
      case All:
        result = findAll();
        break;
    }
    return result;
  }

  @Override
  public LeasePath find(Finder<LeasePath> finder, Object... params) {
    LeasePathFinder lFinder = (LeasePathFinder) finder;
    LeasePath result = null;

    switch (lFinder) {
      case ByPKey:
        String path = (String) params[0];
        result = findByPKey(path);
        break;
    }

    return result;
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(LeasePath lPath) throws TransactionContextException {
    if (removedLPaths.containsKey(lPath)) {
      throw new TransactionContextException("Removed lease-path passed to be persisted");
    }

    modifiedLPaths.put(lPath, lPath);
    leasePaths.put(lPath, lPath);
    pathToLeasePath.put(lPath.getPath(), lPath);
    if (allLeasePathsRead) {
      if (holderLeasePaths.containsKey(lPath.getHolderId())) {
        holderLeasePaths.get(lPath.getHolderId()).add(lPath);
      } else {
        TreeSet<LeasePath> lSet = new TreeSet<LeasePath>();
        lSet.add(lPath);
        holderLeasePaths.put(lPath.getHolderId(), lSet);
      }
    }
  }

  @Override
  public void add(LeasePath entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void commit() {
    for (LeasePath lp : modifiedLPaths.values()) {
      LeasePathsTable lTable = session.newInstance(LeasePathsTable.class);
      LeasePathFactory.createPersistableLeasePathInstance(lp, lTable);
      session.savePersistent(lTable);
    }

    for (LeasePath lp : removedLPaths.values()) {
      LeasePathsTable lTable = session.newInstance(LeasePathsTable.class, lp.getPath());
      session.deletePersistent(lTable);
    }
  }

  private Collection<LeasePath> findByHolderId(int holderId) {
    if (holderLeasePaths.containsKey(holderId)) {
      return holderLeasePaths.get(holderId);
    } else {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<LeasePathsTable> dobj = qb.createQueryDefinition(LeasePathsTable.class);
      dobj.where(dobj.get("holderID").equal(dobj.param("param")));
      Query<LeasePathsTable> query = session.createQuery(dobj);
      query.setParameter("param", holderId);
      List<LeasePathsTable> paths = query.getResultList();
      TreeSet<LeasePath> lpSet = syncLeasePathInstances(paths, false);
      holderLeasePaths.put(holderId, lpSet);

      return lpSet;
    }
  }

  private TreeSet<LeasePath> syncLeasePathInstances(List<LeasePathsTable> lpTables, boolean allRead) {
    TreeSet<LeasePath> finalList = new TreeSet<LeasePath>();

    for (LeasePathsTable lpt : lpTables) {
      LeasePath lPath = LeasePathFactory.createLeasePath(lpt);
      if (!removedLPaths.containsKey(lPath)) {
        if (this.leasePaths.containsKey(lPath)) {
          lPath = this.leasePaths.get(lPath);
        } else {
          this.leasePaths.put(lPath, lPath);
          this.pathToLeasePath.put(lpt.getPath(), lPath);
        }
        finalList.add(lPath);
        if (allRead) {
          if (holderLeasePaths.containsKey(lPath.getHolderId())) {
            holderLeasePaths.get(lPath.getHolderId()).add(lPath);
          } else {
            TreeSet<LeasePath> lSet = new TreeSet<LeasePath>();
            lSet.add(lPath);
            holderLeasePaths.put(lPath.getHolderId(), lSet);
          }
        }
      }
    }

    return finalList;
  }

  private LeasePath findByPKey(String path) {
    if (pathToLeasePath.containsKey(path)) {
      return pathToLeasePath.get(path);
    }

    LeasePathsTable lPTable = session.find(LeasePathsTable.class, path);
    LeasePath lPath = null;
    if (lPTable != null) {
      lPath = LeasePathFactory.createLeasePath(lPTable);
      leasePaths.put(lPath, lPath);
      pathToLeasePath.put(lPath.getPath(), lPath);
    }
    return lPath;
  }

  private Collection<LeasePath> findByPrefix(String prefix) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType dobj = qb.createQueryDefinition(LeasePathsTable.class);
    PredicateOperand propertyPredicate = dobj.get("path");
    String param = "prefix";
    PredicateOperand propertyLimit = dobj.param(param);
    Predicate like = propertyPredicate.like(propertyLimit);
    dobj.where(like);
    Query query = session.createQuery(dobj);
    query.setParameter(param, prefix + "%");
    List<LeasePathsTable> resultset = query.getResultList();
    if (resultset != null) {
      return syncLeasePathInstances(resultset, false);
    }

    return null;
  }

  private Collection<LeasePath> findAll() {
    if (allLeasePathsRead) {
      return new TreeSet<LeasePath>(leasePaths.values());
    }

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType dobj = qb.createQueryDefinition(LeasePathsTable.class);
    Query query = session.createQuery(dobj);
    List<LeasePathsTable> resultset = query.getResultList();
    TreeSet<LeasePath> lPathSet = syncLeasePathInstances(resultset, true);
    allLeasePathsRead = true;
    return lPathSet;
  }
}
