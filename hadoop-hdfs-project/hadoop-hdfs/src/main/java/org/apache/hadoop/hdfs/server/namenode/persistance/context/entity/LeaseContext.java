package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseContext extends EntityContext<Lease> {

    /**
     * Lease
     */
    private Map<String, Lease> leases = new HashMap<String, Lease>();
    private Map<Integer, Lease> idToLease = new HashMap<Integer, Lease>();
    private Map<Lease, Lease> newLeases = new HashMap<Lease, Lease>();
    private Map<Lease, Lease> modifiedLeases = new HashMap<Lease, Lease>();
    private Map<Lease, Lease> removedLeases = new HashMap<Lease, Lease>();
    private boolean allLeasesRead = false;
    private int byHoldernullCount = 0;
    private int byIdNullCount = 0;
    private LeaseDataAccess dataAccess;

    public LeaseContext(LeaseDataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    @Override
    public void add(Lease lease) throws PersistanceException {
        if (removedLeases.containsKey(lease)) {
            throw new TransactionContextException("Removed lease passed to be persisted");
        }

        if (leases.containsKey(lease.getHolder()) && leases.get(lease.getHolder()) == null) {
            byHoldernullCount--;
        }

        if (idToLease.containsKey(lease.getHolderID()) && idToLease.get(lease.getHolderID()) == null) {
            byIdNullCount--;
        }
        newLeases.put(lease, lease);
        leases.put(lease.getHolder(), lease);
        idToLease.put(lease.getHolderID(), lease);
        log("added-lease", CacheHitState.NA, new String[]{"holder", lease.getHolder()});
    }

    @Override
    public void clear() {
        idToLease.clear();
        newLeases.clear();
        modifiedLeases.clear();
        removedLeases.clear();
        leases.clear();
        allLeasesRead = false;
        byHoldernullCount = 0;
        byIdNullCount = 0;
    }

    @Override
    public int count(CounterType<Lease> counter, Object... params) throws PersistanceException {
        Lease.Counter lCounter = (Lease.Counter) counter;
        switch (lCounter) {
            case All:
                if (allLeasesRead) {
                    log("count-all-leases", CacheHitState.HIT);
                    return leases.size() - byHoldernullCount;
                } else {
                    log("count-all-leases", CacheHitState.LOSS);
                    return dataAccess.countAll();
                }
        }

        throw new RuntimeException(UNSUPPORTED_COUNTER);
    }

    @Override
    public Lease find(FinderType<Lease> finder, Object... params) throws PersistanceException {
        Lease.Finder lFinder = (Lease.Finder) finder;
        Lease result = null;
        switch (lFinder) {
            case ByPKey:
                String holder = (String) params[0];
                if (leases.containsKey(holder)) {
                    log("find-lease-by-pk", CacheHitState.HIT, new String[]{"holder", holder});
                    result = leases.get(holder);
                } else {
                    log("find-lease-by-pk", CacheHitState.LOSS, new String[]{"holder", holder});
                    result = dataAccess.findByPKey(holder);
                    if (result == null) {
                        byHoldernullCount++;
                    } else {
                        idToLease.put(result.getHolderID(), result);
                    }
                    leases.put(holder, result);
                }
                return result;
            case ByHolderId:
                int holderId = (Integer) params[0];
                if (idToLease.containsKey(holderId)) {
                    log("find-lease-by-holderid", CacheHitState.HIT, new String[]{"hid", Integer.toString(holderId)});
                    result = idToLease.get(holderId);
                } else {
                    log("find-lease-by-holderid", CacheHitState.LOSS, new String[]{"hid", Integer.toString(holderId)});
                    result = dataAccess.findByHolderId(holderId);
                    if (result == null) {
                        byIdNullCount++;
                    } else {
                        leases.put(result.getHolder(), result);
                    }
                    idToLease.put(holderId, result);
                }
                return result;
        }

        throw new RuntimeException(UNSUPPORTED_FINDER);
    }

    @Override
    public Collection<Lease> findList(FinderType<Lease> finder, Object... params) throws PersistanceException {
        Lease.Finder lFinder = (Lease.Finder) finder;
        Collection<Lease> result = null;
        switch (lFinder) {
            case ByTimeLimit:
                long timeLimit = (Long) params[0];
                log("find-leases-by-timelimit", CacheHitState.NA, new String[]{"timelimit", Long.toString(timeLimit)});
                result = syncLeaseInstances(dataAccess.findByTimeLimit(timeLimit));
                return result;
            case All:
                if (allLeasesRead) {
                    log("find-all-leases", CacheHitState.HIT);
                    result = new TreeSet<Lease>();
                    for (Lease l : leases.values()) {
                        if (l != null) {
                            result.add(l);
                        }
                    }
                } else {
                    log("find-all-leases", CacheHitState.LOSS);
                    result = syncLeaseInstances(dataAccess.findAll());
                    allLeasesRead = true;
                }
                return result;
        }
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }

    @Override
    public void prepare() throws StorageException {
        dataAccess.prepare(removedLeases.values(), newLeases.values(), modifiedLeases.values());
    }

    @Override
    public void remove(Lease lease) throws PersistanceException {
        if (leases.remove(lease.getHolder()) == null) {
            throw new TransactionContextException("Unattached lease passed to be removed");
        }
        idToLease.remove(lease.getHolderID());
        newLeases.remove(lease);
        modifiedLeases.remove(lease);
        removedLeases.put(lease, lease);
        log("removed-lease", CacheHitState.NA, new String[]{"holder", lease.getHolder()});
    }

    @Override
    public void removeAll() throws PersistanceException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void update(Lease lease) throws PersistanceException {
        if (removedLeases.containsKey(lease)) {
            throw new TransactionContextException("Removed lease passed to be persisted");
        }

        modifiedLeases.put(lease, lease);
        leases.put(lease.getHolder(), lease);
        idToLease.put(lease.getHolderID(), lease);
        log("updated-lease", CacheHitState.NA, new String[]{"holder", lease.getHolder()});
    }

    private SortedSet<Lease> syncLeaseInstances(Collection<Lease> list) {
        SortedSet<Lease> finalSet = new TreeSet<Lease>();
        for (Lease lease : list) {
            if (!removedLeases.containsKey(lease)) {
                if (leases.containsKey(lease.getHolder())) {
                    if (leases.get(lease.getHolder()) == null) {
                        byHoldernullCount--;
                        leases.put(lease.getHolder(), lease);
                    }
                    finalSet.add(leases.get(lease.getHolder()));
                } else {
                    finalSet.add(lease);
                    leases.put(lease.getHolder(), lease);
                }

                if (idToLease.containsKey(lease.getHolderID())) {
                    if (idToLease.get(lease.getHolderID()) == null) {
                        byIdNullCount--;
                        idToLease.put(lease.getHolderID(), lease);
                    }
                } else {
                    idToLease.put(lease.getHolderID(), lease);
                }
            }
        }

        return finalSet;
    }
}
