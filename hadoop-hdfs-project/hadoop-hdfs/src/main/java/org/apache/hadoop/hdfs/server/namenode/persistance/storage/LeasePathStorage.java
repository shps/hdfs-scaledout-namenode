package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class LeasePathStorage implements Storage<LeasePath> {

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
}
