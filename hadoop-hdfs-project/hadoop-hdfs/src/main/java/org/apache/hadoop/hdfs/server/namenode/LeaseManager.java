/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.mysql.clusterj.ClusterJException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

import static org.apache.hadoop.hdfs.server.common.Util.now;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 * 
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp from the namenode
 * 2.4) p gets the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
@InterfaceAudience.Private
public class LeaseManager {

  public static final Log LOG = LogFactory.getLog(LeaseManager.class);
  private final FSNamesystem fsnamesystem;
  private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = HdfsConstants.LEASE_HARDLIMIT_PERIOD;
  //
  // Used for handling lock-leases
  // Mapping: leaseHolder -> Lease
  //
  private SortedMap<String, Lease> leases = new TreeMap<String, Lease>();
  // Set of: Lease
  private SortedSet<Lease> sortedLeases = new TreeSet<Lease>();
  // 
  // Map path names to leases. It is protected by the sortedLeases lock.
  // The map stores pathnames in lexicographical order.
  //
  private SortedMap<String, Lease> sortedLeasesByPath = new TreeMap<String, Lease>();
  private EntityManager em = EntityManager.getInstance();

  LeaseManager(FSNamesystem fsnamesystem) {
    this.fsnamesystem = fsnamesystem;
  }

  Lease getLease(String holder) {
    return LeaseHelper.getLease(holder);
  }

  Lease getLeaseOld(String holder) {
    return leases.get(holder);
  }

  static SortedSet<Lease> getSortedLeases() {
    return LeaseHelper.getSortedLeases();
  }

  @Deprecated
  SortedSet<Lease> getSortedLeasesOld() {
    return sortedLeases;
  }

  /** @return the lease containing src */
  public Lease getLeaseByPath(String src) {
    return LeaseHelper.getLeaseByPath(src);
  }

  @Deprecated
  public Lease getLeaseByPathOld(String src) {
    return sortedLeasesByPath.get(src);
  }

  /** @return the number of leases currently in the system */
  public synchronized int countLease() {
    return LeaseHelper.getSortedLeases().size();
  }

  @Deprecated
  public synchronized int countLeaseOld() {
    return sortedLeases.size();
  }

  /** This method is never called in the stateless implementation 
   * @return the number of paths contained in all leases 
   * */
  @Deprecated
  synchronized int countPath() {
    int count = 0;
    for (Lease lease : sortedLeases) {
      count += lease.getPaths().size();
    }
    return count;
  }

  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  synchronized Lease addLease(String holder, String src,
          boolean isTransactional) {
    Lease lease = getLease(holder);
    if (lease == null) {
      int holderID = DFSUtil.getRandom().nextInt();
      lease = LeaseHelper.addLease(holder, holderID, src, now(),
              isTransactional);
    } else {
      lease = LeaseHelper.renewLease(holder,
              lease.getHolderID(), src, isTransactional);
    }
    
    LeasePath lPath = new LeasePath(src, lease.getHolderID());
    lease.getPaths().add(lPath);
    em.persist(em);
    
    return lease;
  }

  /**
   * Remove the specified lease and src
   */
  synchronized void removeLease(Lease lease, LeasePath src, boolean isTransactional) {
    if (lease.getPaths().remove(src))
      LOG.error(src + " not found in lease.paths (=" + lease.paths + ")");
    em.remove(src);

    if (!lease.hasPath()) {
      LeaseHelper.deleteLease(lease.getHolder(), isTransactional);

    }
  }

  @Deprecated
  synchronized void removeLeaseOld(Lease lease, String src, boolean isTransactional) {
//    sortedLeasesByPath.remove(src);
//    if (!lease.removePath(src, isTransactional)) {
//      LOG.error(src + " not found in lease.paths (=" + lease.paths + ")");
//    }

    if (!lease.hasPath()) {
      leases.remove(lease.holder);
      if (!sortedLeases.remove(lease)) {
        LOG.error(lease + " not found in sortedLeases");
      }
    }
  }

  /**
   * Remove the lease for the specified holder and src
   */
  synchronized void removeLease(String holder, String src, boolean isTransactional) {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, new LeasePath(src, lease.getHolderID()), isTransactional);
    }
  }

  /**
   * Reassign lease for file src to the new holder.
   */
  synchronized Lease reassignLease(Lease lease, String src, String newHolder, boolean isTransactional) {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      removeLease(lease, new LeasePath(src, lease.getHolderID()), isTransactional);
    }
    return addLease(newHolder, src, isTransactional);
  }

  /**
   * Finds the pathname for the specified pendingFile
   */
  public synchronized String findPath(INodeFileUnderConstruction pendingFile)
          throws IOException {
    Lease lease = getLease(pendingFile.getClientName());
    if (lease != null) {
      String src = lease.findPath(pendingFile);
      if (src != null) {
        return src;
      }
    }
    throw new IOException("pendingFile (=" + pendingFile + ") not found."
            + "(lease=" + lease + ")");
  }

  /**
   * Renew the lease(s) held by the given client
   */
  synchronized void renewLease(String holder, boolean transactional) {
    renewLease(getLease(holder), transactional);
  }

  synchronized void renewLease(Lease lease, boolean isTransactional) {
    if (lease != null) {
      lease.renew(isTransactional);
    }
  }

  synchronized void renewLeaseOld(Lease lease) {
    if (lease != null) {
      sortedLeases.remove(lease);
      lease.renew(false);
      sortedLeases.add(lease);
    }
  }

  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  public class Lease implements Comparable<Lease> {

    private final String holder;
    private long lastUpdate;
    //private final Collection<String> paths = new TreeSet<String>();
    private Collection<LeasePath> paths = new TreeSet<LeasePath>();
    private int holderID; // KTHFS
    private EntityManager em = EntityManager.getInstance();

    /*Only LeaseManager object can create a lease */
    /**
     * @deprecated W: because Lease objects need to be created from Helper functions
     * */
    private Lease(String holder) {
      this.holder = holder;
      renew(false);
    }

    /*W: This constructor should be used when lazy fetching is done with renew = false*/
    public Lease(String holder, int holderID, long lastUpd) {
      this.holder = holder;
      this.holderID = holderID;
      this.lastUpdate = lastUpd;
      /*			if(renew)
      renew();*/
    }

    /** W: Added for ThesisFS */
    public void setLastUpdate(long lastUpd) {
      this.lastUpdate = lastUpd;
    }

    public void setPaths(TreeSet<LeasePath> paths) {
      this.paths = paths;
    }

    public long getLastUpdated() {
      return this.lastUpdate;
    }

    public void setHolderID(int holderID) {
      this.holderID = holderID;
    }

    public int getHolderID() {
      return this.holderID;
    }

    /** Only LeaseManager object can renew a lease */
    private void renew(boolean isTransactional) {
      this.lastUpdate = now(); //W: this might not be required because we always read lastUpdate from the DB
      LeaseHelper.renewLease(this.holder, isTransactional);

    }
    
    public void removePath(LeasePath lPath)
    {
      getPaths().remove(lPath);
    }

    @Deprecated
    private void renewOld() {
      this.lastUpdate = now();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      return now() - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return now() - lastUpdate > softLimit;
    }

    /**
     * @return the path associated with the pendingFile and null if not found.
     */
    private String findPath(INodeFileUnderConstruction pendingFile) {
      try {
          
        for (LeasePath lpath : this.getPaths()) {
          if (fsnamesystem.dir.getFileINode(lpath.getPath()).getFullPathName().equals(pendingFile.getFullPathName())) {
            return lpath.getPath();
          }
        }
      } catch (UnresolvedLinkException e) {
        throw new AssertionError("Lease files should reside on this FS");
      }
      return null;
    }

    /** Does this lease contain any path? */
    boolean hasPath() {
      return !this.getPaths().isEmpty();
    }

    /** {@inheritDoc} */
    public String toString() {
      return "[Lease.  Holder: " + holder
              + ", pendingcreates: " + paths.size() + "]";
    }

    /** {@inheritDoc} */
    public int compareTo(Lease o) {
      Lease l1 = this;
      Lease l2 = o;
      long lu1 = l1.lastUpdate;
      long lu2 = l2.lastUpdate;
      if (lu1 < lu2) {
        return -1;
      } else if (lu1 > lu2) {
        return 1;
      } else {
        return l1.holder.compareTo(l2.holder);
      }
    }

    /** {@inheritDoc} */
    public boolean equals(Object o) {
      if (!(o instanceof Lease)) {
        return false;
      }
      Lease obj = (Lease) o;
      if (lastUpdate == obj.lastUpdate
              && holder.equals(obj.holder)) {
        return true;
      }
      return false;
    }

    /** {@inheritDoc} */
    public int hashCode() {
      return holder.hashCode();
    }

    Collection<LeasePath> getPaths() {
      if (paths == null)
        paths = em.findLeasePathsByHolder(holderID);
      
      return paths;
    }

    String getHolder() {
      return holder;
    }

    void replacePath(LeasePath oldpath, LeasePath newpath) {
      getPaths().remove(oldpath);
      getPaths().add(newpath);
    }

    boolean doesExists() {
      return LeaseHelper.exists(holder);
    }
  }

  synchronized void changeLease(String src, String dst,
          String overwrite, String replaceBy, boolean isTransactional) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: "
              + " src=" + src + ", dest=" + dst
              + ", overwrite=" + overwrite
              + ", replaceBy=" + replaceBy);
    }

    final int len = overwrite.length();
    SortedMap<LeasePath, Lease> sortedLeasesByPathFromDB = LeaseHelper.getSortedLeasesByPath();

    for (Map.Entry<LeasePath, Lease> entry : findLeaseWithPrefixPath(src, sortedLeasesByPathFromDB)) {
      final LeasePath oldpath = entry.getKey();
      final Lease lease = entry.getValue();
      //overwrite must be a prefix of oldpath
      final LeasePath newpath = new LeasePath(replaceBy + oldpath.getPath().substring(len), lease.getHolderID());
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldpath + " with " + newpath);
      }
      lease.replacePath(oldpath, newpath);
    }
  }

  synchronized void removeLeaseWithPrefixPath(String prefix, boolean isTransactional) {
    SortedMap<LeasePath, Lease> sortedLeasesByPathFromDB = LeaseHelper.getSortedLeasesByPath();
    for (Map.Entry<LeasePath, Lease> entry : findLeaseWithPrefixPath(prefix, sortedLeasesByPathFromDB)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName()
                + ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue(), entry.getKey(), isTransactional);
    }
  }

  static private List<Map.Entry<LeasePath, Lease>> findLeaseWithPrefixPath(
          String prefix, SortedMap<LeasePath, Lease> path2lease) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    List<Map.Entry<LeasePath, Lease>> entries = new ArrayList<Map.Entry<LeasePath, Lease>>();
    final int srclen = prefix.length();

    for (Map.Entry<LeasePath, Lease> entry : 
            path2lease.tailMap(new LeasePath(prefix, Integer.MIN_VALUE)).entrySet()) {
      final LeasePath p = entry.getKey();
      if (!p.getPath().startsWith(prefix)) {
        return entries;
      }
      if (p.getPath().length() == srclen || p.getPath().charAt(srclen) == Path.SEPARATOR_CHAR) {
        entries.add(entry);
      }
    }
    return entries;
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit;
  }

  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  class Monitor implements Runnable {

    final String name = getClass().getSimpleName();

    /** Check leases periodically. */
    public void run() {
      for (; fsnamesystem.isRunning();) {
        fsnamesystem.writeLock();
        try {
          if (!fsnamesystem.isInSafeMode()) {
            checkLeases();
          }
        } finally {
          fsnamesystem.writeUnlock();
        }


        try {
          Thread.sleep(HdfsServerConstants.NAMENODE_LEASE_RECHECK_INTERVAL);
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        }
      }
    }
  }

  /** Check the leases beginning from the oldest. */
  private synchronized void checkLeases() {
    assert fsnamesystem.hasWriteLock();
    SortedSet<Lease> sortedLeasesFromDB = LeaseHelper.getSortedLeases();
    for (; sortedLeasesFromDB.size() > 0;) {
      final Lease oldest = sortedLeasesFromDB.first();
      if (!oldest.expiredHardLimit()) {
        return;
      }

      LOG.info("Lease " + oldest + " has expired hard limit");

      final List<LeasePath> removing = new ArrayList<LeasePath>();
      // need to create a copy of the oldest lease paths, becuase 
      // internalReleaseLease() removes paths corresponding to empty files,
      // i.e. it needs to modify the collection being iterated over
      // causing ConcurrentModificationException
      Collection<LeasePath> paths = oldest.getPaths();
      assert paths != null : "The lease " + oldest.toString() + " has no path.";
      LeasePath[] leasePaths = new LeasePath[paths.size()];
      paths.toArray(leasePaths);
      for (LeasePath lPath : leasePaths) {
        try {
          if (fsnamesystem.internalReleaseLease(oldest, lPath.getPath(), HdfsServerConstants.NAMENODE_LEASE_HOLDER, false))
          {
            LOG.info("Lease recovery for file " + lPath
                    + " is complete. File closed.");
            removing.add(lPath);
          } else {
            LOG.info("Started block recovery for file " + lPath
                    + " lease " + oldest);
          }
        } catch (IOException e) {
          LOG.error("Cannot release the path " + lPath + " in the lease " + oldest, e);
          removing.add(lPath);
        }
      }

      for (LeasePath lPath : removing) {
        boolean isDone = false;
        int tries = DBConnector.RETRY_COUNT;

        try {
          while (!isDone && tries > 0) {
            try {
              DBConnector.beginTransaction();
              removeLease(oldest, lPath, true);
              DBConnector.commit();
              isDone = true;
            } catch (ClusterJException ex) {
              if (!isDone) {
                DBConnector.safeRollback();
                tries--;
                FSNamesystem.LOG.error("removeLease() :: failed to remove lease from holder" + oldest.getHolder() + " on file " + lPath + ". Exception: " + ex.getMessage(), ex);
              }
            }
          }
        } finally {
          if (!isDone) {
            DBConnector.safeRollback();
          }
        }
      }
      sortedLeasesFromDB = LeaseHelper.getSortedLeases();
    }
  }

  /** {@inheritDoc} */
  public synchronized String toString() {
    return getClass().getSimpleName() + "= {"
            //+ "\n leases=" + leases //TODO: helper function required to cook a HashMap of <holder, Lease>
            + "\n sortedLeases=" + LeaseHelper.getSortedLeases()
            + "\n sortedLeasesByPath=" + LeaseHelper.getSortedLeasesByPath()
            + "\n}";
  }

  @Deprecated
  public synchronized String toStringOld() {
    return getClass().getSimpleName() + "= {"
            + "\n leases=" + leases
            + "\n sortedLeases=" + sortedLeases
            + "\n sortedLeasesByPath=" + sortedLeasesByPath
            + "\n}";
  }
}
