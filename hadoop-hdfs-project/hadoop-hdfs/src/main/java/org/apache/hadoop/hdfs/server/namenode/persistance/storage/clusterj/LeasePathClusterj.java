package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.List;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.LeasePathFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.LeasePathStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathClusterj extends LeasePathStorage {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public void commit() {
    for (LeasePath lp : newLPaths.values()) {
      LeasePathsTable lTable = session.newInstance(LeasePathsTable.class);
      LeasePathFactory.createPersistableLeasePathInstance(lp, lTable);
      session.savePersistent(lTable);
    }

    for (LeasePath lp : removedLPaths.values()) {
      LeasePathsTable lTable = session.newInstance(LeasePathsTable.class, lp.getPath());
      session.deletePersistent(lTable);
    }
  }

  @Override
  protected TreeSet<LeasePath> findByHolderId(int holderId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeasePathsTable> dobj = qb.createQueryDefinition(LeasePathsTable.class);
    dobj.where(dobj.get("holderId").equal(dobj.param("param")));
    Query<LeasePathsTable> query = session.createQuery(dobj);
    query.setParameter("param", holderId);
    List<LeasePathsTable> paths = query.getResultList();
    TreeSet<LeasePath> lpSet = syncLeasePathInstances(paths, false);

    return lpSet;
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

  @Override
  protected LeasePath findByPKey(String path) {
    LeasePathsTable lPTable = session.find(LeasePathsTable.class, path);
    LeasePath lPath = null;
    if (lPTable != null) {
      lPath = LeasePathFactory.createLeasePath(lPTable);
    }
    return lPath;
  }

  @Override
  protected TreeSet<LeasePath> findByPrefix(String prefix) {
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

  @Override
  protected TreeSet<LeasePath> findAll() {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType dobj = qb.createQueryDefinition(LeasePathsTable.class);
    Query query = session.createQuery(dobj);
    List<LeasePathsTable> resultset = query.getResultList();
    TreeSet<LeasePath> lPathSet = syncLeasePathInstances(resultset, true);
    return lPathSet;
  }
}
