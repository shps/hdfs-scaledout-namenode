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
  private LeaseDataAccess dataAccess;

  public LeaseContext(LeaseDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(Lease lease) throws PersistanceException {
    if (removedLeases.containsKey(lease)) {
      throw new TransactionContextException("Removed lease passed to be persisted");
    }

    newLeases.put(lease, lease);
    leases.put(lease.getHolder(), lease);
    idToLease.put(lease.getHolderID(), lease);
  }

  @Override
  public void clear() {
    idToLease.clear();
    newLeases.clear();
    modifiedLeases.clear();
    removedLeases.clear();
    leases.clear();
    allLeasesRead = false;
  }

  @Override
  public int count(CounterType<Lease> counter, Object... params) throws PersistanceException {
    Lease.Counter lCounter = (Lease.Counter) counter;
    switch (lCounter) {
      case All:
        return dataAccess.countAll();
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
          result = leases.get(holder);
        } else {
          result = dataAccess.findByPKey(holder);
          if (result != null) {
            leases.put(result.getHolder(), result);
          }
        }
        return result;
      case ByHolderId:
        int holderId = (Integer) params[0];
        if (idToLease.containsKey(holderId)) {
          result = idToLease.get(holderId);
        } else {
          result = dataAccess.findByHolderId(holderId);
          if (result != null) {
            leases.put(result.getHolder(), result);
            idToLease.put(result.getHolderID(), result);
          }
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
        result = syncLeaseInstances(dataAccess.findByTimeLimit(timeLimit));
        return result;
      case All:
        if (allLeasesRead) {
          result = new TreeSet<Lease>(this.leases.values());
        } else {
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
  }

  private SortedSet<Lease> syncLeaseInstances(Collection<Lease> list) {
    SortedSet<Lease> finalSet = new TreeSet<Lease>();
    for (Lease lease : list) {
      if (!removedLeases.containsKey(lease)) {
        if (leases.containsKey(lease.getHolder())) {
          finalSet.add(leases.get(lease.getHolder()));
        } else {
          finalSet.add(lease);
          leases.put(lease.getHolder(), lease);
          idToLease.put(lease.getHolderID(), lease);
        }
      }
    }

    return finalSet;
  }
}
