/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.AbstractMap.SimpleEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.persistance.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager.LockType;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

/**
 * LeaseManager does the lease housekeeping for writing on files. This class
 * also provides useful static methods for lease recovery.
 *
 * Lease Recovery Algorithm 1) Namenode retrieves lease information 2) For each
 * file f in the lease, consider the last block b of f 2.1) Get the datanodes
 * which contains b 2.2) Assign one of the datanodes as the primary datanode p
 *
 * 2.3) p obtains a new generation stamp from the namenode 2.4) p gets the block
 * info from each datanode 2.5) p computes the minimum block length 2.6) p
 * updates the datanodes, which have a valid generation stamp, with the new
 * generation stamp and the minimum block length 2.7) p acknowledges the
 * namenode the update results
 *
 * 2.8) Namenode updates the BlockInfo 2.9) Namenode removes f from the lease
 * and removes the lease once all files have been removed 2.10) Namenode commit
 * changes to edit log
 */
@InterfaceAudience.Private
public class LeaseManager {

  public static final Log LOG = LogFactory.getLog(LeaseManager.class);
  private final FSNamesystem fsnamesystem;
  private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = HdfsConstants.LEASE_HARDLIMIT_PERIOD;

  LeaseManager(FSNamesystem fsnamesystem) {
    this.fsnamesystem = fsnamesystem;
  }

  Lease getLease(String holder) throws PersistanceException {
    return EntityManager.find(Lease.Finder.ByPKey, holder);
  }

  Collection<Lease> getSortedLeases() throws PersistanceException {
    return EntityManager.findList(Lease.Finder.All);
  }

  /**
   * @return the lease containing src
   */
  public Lease getLeaseByPath(String src) throws PersistanceException {
    LeasePath leasePath = EntityManager.find(LeasePath.Finder.ByPKey, src);
    if (leasePath != null) {
      int holderID = leasePath.getHolderId();
      Lease lease = EntityManager.find(Lease.Finder.ByHolderId, holderID);
      return lease;
    } else {
      return null;
    }
  }

  /**
   * @return the number of leases currently in the system
   */
  public int countLease() throws IOException {
    return (int) new LightWeightRequestHandler(OperationType.COUNT_LEASE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        LeaseDataAccess da = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /**
   * This method is never called in the stateless implementation
   *
   * @return the number of paths contained in all leases
   *
   */
  int countPath() throws PersistanceException {
    return EntityManager.count(Lease.Counter.All);
  }

  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  Lease addLease(String holder, String src) throws PersistanceException {
    Lease lease = getLease(holder);
    if (lease == null) {
      int holderID = DFSUtil.getRandom().nextInt();
      lease = new Lease(holder, holderID, now());
      EntityManager.add(lease);
    } else {
      renewLease(lease);
    }

    LeasePath lPath = new LeasePath(src, lease.getHolderID());
    lease.addFirstPath(lPath);
    EntityManager.add(lPath);

    return lease;
  }

  /**
   * Remove the specified lease and src
   */
  void removeLease(Lease lease, LeasePath src) throws PersistanceException {
    if (lease.removePath(src)) {
      EntityManager.remove(src);
    } else {
      LOG.error(src + " not found in lease.paths (=" + lease.getPaths() + ")");
    }

    if (!lease.hasPath()) {
      EntityManager.remove(lease);

    }
  }

  /**
   * Remove the lease for the specified holder and src
   */
  void removeLease(String holder, String src) throws PersistanceException {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, new LeasePath(src, lease.getHolderID()));
    }
  }

  /**
   * Reassign lease for file src to the new holder.
   */
  Lease reassignLease(Lease lease, String src, String newHolder) throws PersistanceException {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      // Removing lease-path souldn't be persisted in entity-manager since we want to add it to another lease.
      if (!lease.removePath(new LeasePath(src, lease.getHolderID()))) {
        LOG.error(src + " not found in lease.paths (=" + lease.getPaths() + ")");
      }

      if (!lease.hasPath() && !lease.getHolder().equals(newHolder)) {
        EntityManager.remove(lease);

      }
    }

    Lease newLease = getLease(newHolder);
    LeasePath lPath = null;
    if (newLease == null) {
      int holderID = DFSUtil.getRandom().nextInt();
      newLease = new Lease(newHolder, holderID, now());
      EntityManager.add(newLease);
      lPath = new LeasePath(src, newLease.getHolderID());
      newLease.addFirstPath(lPath); // [lock] First time, so no need to look for lease-paths
    } else {
      renewLease(newLease);
      lPath = new LeasePath(src, newLease.getHolderID());
      newLease.addPath(lPath);
    }
    // update lease-paths' holder
    EntityManager.update(lPath);

    return newLease;
  }

  /**
   * Finds the pathname for the specified pendingFile
   */
  public String findPath(INodeFile pendingFile)
          throws IOException, PersistanceException {
    assert pendingFile.isUnderConstruction();
    Lease lease = getLease(pendingFile.getClientName());
    if (lease != null) {
      String src = null;
      try {
        for (LeasePath lpath : lease.getPaths()) {
          if (fsnamesystem.dir.getFileINode(lpath.getPath()).getFullPathName().equals(pendingFile.getFullPathName())) {
            src = lpath.getPath();
            break;
          }
        }
      } catch (UnresolvedLinkException e) {
        throw new AssertionError("Lease files should reside on this FS");
      }
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
  void renewLease(String holder) throws PersistanceException {
    renewLease(getLease(holder));
  }

  void renewLease(Lease lease) throws PersistanceException {
    if (lease != null) {
      lease.setLastUpdate(now());
      EntityManager.update(lease);
    }
  }

  void changeLease(String src, String dst,
          String overwrite, String replaceBy) throws PersistanceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: "
              + " src=" + src + ", dest=" + dst
              + ", overwrite=" + overwrite
              + ", replaceBy=" + replaceBy);
    }

    final int len = overwrite.length();

    for (Map.Entry<LeasePath, Lease> entry : findLeaseWithPrefixPath(src)) {
      final LeasePath oldPath = entry.getKey();
      final Lease lease = entry.getValue();
      //overwrite must be a prefix of oldpath
      final LeasePath newPath = new LeasePath(replaceBy + oldPath.getPath().substring(len), lease.getHolderID());
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldPath + " with " + newPath);
      }
      lease.replacePath(oldPath, newPath);
      EntityManager.remove(oldPath);
      EntityManager.add(newPath);
    }
  }

  void removeLeaseWithPrefixPath(String prefix) throws PersistanceException {
    for (Map.Entry<LeasePath, Lease> entry : findLeaseWithPrefixPath(prefix)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName()
                + ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue(), entry.getKey());
    }
  }

  private List<Map.Entry<LeasePath, Lease>> findLeaseWithPrefixPath(
          String prefix) throws PersistanceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    Collection<LeasePath> leasePathSet = EntityManager.findList(LeasePath.Finder.ByPrefix, prefix);
    List<Map.Entry<LeasePath, Lease>> entries = new ArrayList<Map.Entry<LeasePath, Lease>>();
    final int srclen = prefix.length();

    for (LeasePath lPath : leasePathSet) {
      if (!lPath.getPath().startsWith(prefix)) {
        LOG.warn("LeasePath fetched by prefix does not start with the prefix: \n"
                + "LeasePath: " + lPath + "\t Prefix: " + prefix);
        return entries;
      }
      if (lPath.getPath().length() == srclen || lPath.getPath().charAt(srclen) == Path.SEPARATOR_CHAR) {
        Lease lease = EntityManager.find(Lease.Finder.ByHolderId, lPath.getHolderId());
        entries.add(new SimpleEntry<LeasePath, Lease>(lPath, lease));
      }
    }
    return entries;
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit;
  }

  /**
   * ****************************************************
   * Monitor checks for leases that have expired, and disposes of them.
   * ****************************************************
   */
  class Monitor implements Runnable {

    final String name = getClass().getSimpleName();

    /**
     * Check leases periodically.
     */
    @Override
    public void run() {
      for (; fsnamesystem.isRunning();) {
        fsnamesystem.writeLock();
        try {
          if (!(Boolean) isInSafeModeHandler.handle()) {
            SortedSet<Lease> sortedLeases = (SortedSet<Lease>) findExpiredLeaseHandler.handle();
            if (sortedLeases != null) {
              for (Lease expiredLease : sortedLeases) {
                expiredLeaseHandler.setParams(expiredLease.getHolder()).handle();
              }
            }
          }
        } catch (IOException ex) {
          LOG.error(ex);
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
    TransactionalRequestHandler isInSafeModeHandler = new TransactionalRequestHandler(OperationType.PREPARE_LEASE_MANAGER_MONITOR) {
      
      @Override
      public Object performTask() throws PersistanceException, IOException {
        return fsnamesystem.isInSafeMode();
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO safemode 
      }
    };
    
    LightWeightRequestHandler findExpiredLeaseHandler = new LightWeightRequestHandler(OperationType.PREPARE_LEASE_MANAGER_MONITOR) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        long expiredTime = now() - hardLimit;
        LeaseDataAccess da = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
        return da.findByTimeLimit(expiredTime);
      }
    };
    
    TransactionalRequestHandler expiredLeaseHandler = new TransactionalRequestHandler(OperationType.LEASE_MANAGER_MONITOR) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        String holder = (String) getParams()[0];
        if (holder != null) {
          checkLeases(holder);
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        String holder = (String) getParams()[0];
        TransactionLockManager tlm = new TransactionLockManager();
        tlm.addINode(TransactionLockManager.INodeLockType.WRITE).
                addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE, holder).
                addNameNodeLease(LockType.WRITE).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                addGenerationStamp(LockType.WRITE).
                acquireByLease(fsnamesystem.getFsDirectory().getRootDir());
      }
    };
  }

  /**
   * Check the leases beginning from the oldest.
   */
  private void checkLeases(String holder) throws PersistanceException {
    Lease oldest = EntityManager.find(Lease.Finder.ByPKey, holder);

    if (oldest == null) {
      return;
    }

    if (!expiredHardLimit(oldest)) {
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
        boolean leaseReleased = false;
        leaseReleased = fsnamesystem.internalReleaseLease(oldest, lPath.getPath(),
                HdfsServerConstants.NAMENODE_LEASE_HOLDER);
        if (leaseReleased) {
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
      if (oldest.getPaths().contains(lPath)) {
        removeLease(oldest, lPath);
      }
    }

//      if (!expiredHardLimit(oldest) || !oldest.hasPath()) // if Lease is renewed or removed
//      {
//        sortedLeases.remove(oldest);
//      }
  }

  private boolean expiredHardLimit(Lease lease) {
    return now() - lease.getLastUpdated() > hardLimit;
  }

  public boolean expiredSoftLimit(Lease lease) {
    return now() - lease.getLastUpdated() > softLimit;
  }
}
