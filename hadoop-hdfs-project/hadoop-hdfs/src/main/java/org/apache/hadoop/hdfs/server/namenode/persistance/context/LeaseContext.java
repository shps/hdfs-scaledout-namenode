package org.apache.hadoop.hdfs.server.namenode.persistance.context;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.FinderType;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class LeaseContext implements EntityContext<Lease> {

  public static final String TABLE_NAME = "leases";
  public static final String HOLDER = "holder";
  public static final String LAST_UPDATE = "last_update";
  public static final String HOLDER_ID = "holder_id";
  /**
   * Lease
   */
  protected Map<String, Lease> leases = new HashMap<String, Lease>();
  protected Map<Integer, Lease> idToLease = new HashMap<Integer, Lease>();
  protected Map<Lease, Lease> modifiedLeases = new HashMap<Lease, Lease>();
  protected Map<Lease, Lease> removedLeases = new HashMap<Lease, Lease>();
  protected boolean allLeasesRead = false;

  @Override
  public void clear() {
    idToLease.clear();
    modifiedLeases.clear();
    removedLeases.clear();
    leases.clear();
    allLeasesRead = false;
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
  public void remove(Lease lease) throws TransactionContextException {
    if (leases.remove(lease.getHolder()) == null) {
      throw new TransactionContextException("Unattached lease passed to be removed");
    }
    idToLease.remove(lease.getHolderID());
    modifiedLeases.remove(lease);
    removedLeases.put(lease, lease);
  }

  @Override
  public Collection<Lease> findList(FinderType<Lease> finder, Object... params) {
    Lease.Finder lFinder = (Lease.Finder) finder;
    Collection<Lease> result = null;
    switch (lFinder) {
      case ByTimeLimit:
        long timeLimit = (Long) params[0];
        result = findByTimeLimit(timeLimit);
        break;
      case All:
        if (allLeasesRead) {
          result = new TreeSet<Lease>(this.leases.values());
        } else {
          result = findAll();
          allLeasesRead = true;
        }
        break;
    }
    return result;
  }

  @Override
  public Lease find(FinderType<Lease> finder, Object... params) {
    Lease.Finder lFinder = (Lease.Finder) finder;
    Lease result = null;
    switch (lFinder) {
      case ByPKey:
        String holder = (String) params[0];
        if (leases.containsKey(holder)) {
          result = leases.get(holder);
        } else {
          result = findByPKey(holder);
          if (result != null) {
            leases.put(result.getHolder(), result);
          }
        }
        break;
      case ByHolderId:
        int holderId = (Integer) params[0];
        if (idToLease.containsKey(holderId)) {
          result = idToLease.get(holderId);
        } else {
          result = findByHolderId(holderId);
          if (result != null) {
            leases.put(result.getHolder(), result);
            idToLease.put(result.getHolderID(), result);
          }
        }
        break;
    }

    return result;
  }

  protected abstract Collection<Lease> findByTimeLimit(long timeLimit);

  protected abstract Collection<Lease> findAll();

  protected abstract Lease findByPKey(String holder);

  protected abstract Lease findByHolderId(int holderId);
}
