package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.Lease;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class LeaseStorage implements Storage<Lease> {

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
}
