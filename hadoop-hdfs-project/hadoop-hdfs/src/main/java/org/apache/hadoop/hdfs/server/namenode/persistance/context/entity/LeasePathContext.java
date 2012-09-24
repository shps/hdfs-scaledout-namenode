package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeasePathDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathContext extends EntityContext<LeasePath> {

    private Map<Integer, Collection<LeasePath>> holderLeasePaths = new HashMap<Integer, Collection<LeasePath>>();
    private Map<LeasePath, LeasePath> leasePaths = new HashMap<LeasePath, LeasePath>();
    private Map<LeasePath, LeasePath> newLPaths = new HashMap<LeasePath, LeasePath>();
    private Map<LeasePath, LeasePath> modifiedLPaths = new HashMap<LeasePath, LeasePath>();
    private Map<LeasePath, LeasePath> removedLPaths = new HashMap<LeasePath, LeasePath>();
    private Map<String, LeasePath> pathToLeasePath = new HashMap<String, LeasePath>();
    private boolean allLeasePathsRead = false;
    private LeasePathDataAccess dataAccess;

    public LeasePathContext(LeasePathDataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    @Override
    public void add(LeasePath lPath) throws PersistanceException {
        if (removedLPaths.containsKey(lPath)) {
            throw new TransactionContextException("Removed lease-path passed to be persisted");
        }

        newLPaths.put(lPath, lPath);
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
        log("added-lpath", CacheHitState.NA,
                new String[]{"path", lPath.getPath(), "hid", Long.toString(lPath.getHolderId())});
    }

    @Override
    public void clear() {
        holderLeasePaths.clear();
        leasePaths.clear();
        newLPaths.clear();
        modifiedLPaths.clear();
        removedLPaths.clear();
        pathToLeasePath.clear();
        allLeasePathsRead = false;
    }

    @Override
    public int count(CounterType counter, Object... params) throws PersistanceException {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public LeasePath find(FinderType<LeasePath> finder, Object... params) throws PersistanceException {
        LeasePath.Finder lFinder = (LeasePath.Finder) finder;
        LeasePath result = null;

        switch (lFinder) {
            case ByPKey:
                String path = (String) params[0];
                if (pathToLeasePath.containsKey(path)) {
                    log("find-lpath-by-pk", CacheHitState.HIT, new String[]{"path", path});
                    result = pathToLeasePath.get(path);
                } else {
                    log("find-lpath-by-pk", CacheHitState.LOSS, new String[]{"path", path});
                    result = dataAccess.findByPKey(path);
                    if (result != null) {
                        leasePaths.put(result, result);
                    }
                    pathToLeasePath.put(path, result);
                }
                return result;
        }

        throw new RuntimeException(UNSUPPORTED_FINDER);
    }

    @Override
    public Collection<LeasePath> findList(FinderType<LeasePath> finder, Object... params) throws PersistanceException {
        LeasePath.Finder lFinder = (LeasePath.Finder) finder;
        Collection<LeasePath> result = null;

        switch (lFinder) {
            case ByHolderId:
                int holderId = (Integer) params[0];
                if (holderLeasePaths.containsKey(holderId)) {
                    log("find-lpaths-by-holderid", CacheHitState.HIT, new String[]{"hid", Long.toString(holderId)});
                    result = holderLeasePaths.get(holderId);
                } else {
                    log("find-lpaths-by-holderid", CacheHitState.LOSS, new String[]{"hid", Long.toString(holderId)});
                    result = syncLeasePathInstances(dataAccess.findByHolderId(holderId), false);
                    holderLeasePaths.put(holderId, result);
                }
                return result;
            case ByPrefix:
                String prefix = (String) params[0];
                log("find-lpaths-by-prefix", CacheHitState.NA, new String[]{"prefix", prefix});
                return syncLeasePathInstances(dataAccess.findByPrefix(prefix), false);
            case All:
                if (allLeasePathsRead) {
                    log("find-all-lpaths", CacheHitState.HIT);
                    result = new TreeSet<LeasePath>();
                    for (LeasePath lp : leasePaths.values()) {
                        if (lp != null) {
                            result.add(lp);
                        }
                    }
                } else {
                    log("find-all-lpaths", CacheHitState.LOSS);
                    result = syncLeasePathInstances(dataAccess.findAll(), true);
                    allLeasePathsRead = true;
                }
                return result;
        }
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }

    @Override
    public void prepare() throws StorageException {
        dataAccess.prepare(removedLPaths.values(), newLPaths.values(), modifiedLPaths.values());
    }

    @Override
    public void remove(LeasePath lPath) throws PersistanceException {
        if (leasePaths.remove(lPath) == null) {
            throw new TransactionContextException("Unattached lease-path passed to be removed");
        }

        pathToLeasePath.remove(lPath.getPath());
        newLPaths.remove(lPath);
        modifiedLPaths.remove(lPath);
        if (holderLeasePaths.containsKey(lPath.getHolderId())) {
            Collection<LeasePath> lSet = holderLeasePaths.get(lPath.getHolderId());
            lSet.remove(lPath);
            if (lSet.isEmpty()) {
                holderLeasePaths.remove(lPath.getHolderId());
            }
        }
        removedLPaths.put(lPath, lPath);
        log("removed-lpath", CacheHitState.NA, new String[]{"path", lPath.getPath()});
    }

    @Override
    public void removeAll() throws PersistanceException {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @Override
    public void update(LeasePath lPath) throws PersistanceException {
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

        log("updated-lpath", CacheHitState.NA,
                new String[]{"path", lPath.getPath(), "hid", Long.toString(lPath.getHolderId())});
    }

    private TreeSet<LeasePath> syncLeasePathInstances(Collection<LeasePath> list, boolean allRead) {
        TreeSet<LeasePath> finalList = new TreeSet<LeasePath>();

        for (LeasePath lPath : list) {
            if (!removedLPaths.containsKey(lPath)) {
                if (leasePaths.containsKey(lPath)) {
                    if (leasePaths.get(lPath) == null) {
                        leasePaths.put(lPath, lPath);
                    }
                    lPath = leasePaths.get(lPath);
                } else {
                    this.leasePaths.put(lPath, lPath);
                }
                if (pathToLeasePath.containsKey(lPath.getPath())) {
                    if (pathToLeasePath.get(lPath.getPath()) == null) {
                        pathToLeasePath.put(lPath.getPath(), lPath);
                    }
                } else {
                    pathToLeasePath.put(lPath.getPath(), lPath);
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
}
