package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class LeasePathContext implements EntityContext<LeasePath> {

  public static final String TABLE_NAME = "lease_paths";
  public static final String HOLDER_ID = "holder_id";
  public static final String PATH = "path";
  protected Map<Integer, TreeSet<LeasePath>> holderLeasePaths = new HashMap<Integer, TreeSet<LeasePath>>();
  protected Map<LeasePath, LeasePath> leasePaths = new HashMap<LeasePath, LeasePath>();
  protected Map<LeasePath, LeasePath> modifiedLPaths = new HashMap<LeasePath, LeasePath>();
  protected Map<LeasePath, LeasePath> removedLPaths = new HashMap<LeasePath, LeasePath>();
  protected Map<String, LeasePath> pathToLeasePath = new HashMap<String, LeasePath>();
  protected boolean allLeasePathsRead = false;

  @Override
  public void clear() {
    holderLeasePaths.clear();
    leasePaths.clear();
    modifiedLPaths.clear();
    removedLPaths.clear();
    pathToLeasePath.clear();
    allLeasePathsRead = false;
  }

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
  public Collection<LeasePath> findList(FinderType<LeasePath> finder, Object... params) {
    LeasePath.Finder lFinder = (LeasePath.Finder) finder;
    TreeSet<LeasePath> result = null;

    switch (lFinder) {
      case ByHolderId:
        int holderId = (Integer) params[0];
        if (holderLeasePaths.containsKey(holderId)) {
          result = holderLeasePaths.get(holderId);
        } else {
          result = findByHolderId(holderId);
          holderLeasePaths.put(holderId, result);
        }
        break;
      case ByPrefix:
        String prefix = (String) params[0];
        result = findByPrefix(prefix);
        break;
      case All:
        if (allLeasePathsRead) {
          result = new TreeSet<LeasePath>(leasePaths.values());
        } else {
          result = findAll();
          allLeasePathsRead = true;
        }
        break;
    }
    return result;
  }

  @Override
  public LeasePath find(FinderType<LeasePath> finder, Object... params) {
    LeasePath.Finder lFinder = (LeasePath.Finder) finder;
    LeasePath result = null;

    switch (lFinder) {
      case ByPKey:
        String path = (String) params[0];
        if (pathToLeasePath.containsKey(path)) {
          result = pathToLeasePath.get(path);
        } else {
          result = findByPKey(path);
          if (result != null) {
            leasePaths.put(result, result);
            pathToLeasePath.put(result.getPath(), result);
          }
        }
        break;
    }

    return result;
  }

  @Override
  public int count(CounterType counter, Object... params) {
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
  public void add(LeasePath lPath) throws TransactionContextException {
    update(lPath);
  }

  protected abstract TreeSet<LeasePath> findByHolderId(int holderId);

  protected abstract TreeSet<LeasePath> findByPrefix(String prefix);

  protected abstract TreeSet<LeasePath> findAll();

  protected abstract LeasePath findByPKey(String path);
}
