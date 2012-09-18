package org.apache.hadoop.hdfs.server.namenode.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.EntityContext.LockMode;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.log4j.NDC;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TransactionLockAcquirer {

  //inode
  private INodeLockType inodeLock = null;
  private Object inodeParam = null;
  private INode inodeResult = null;
  private INode rootDir = null;
  //block
  private LockType blockLock = null;
  private Object blockParam = null;
  private List<BlockInfo> blockResults = null;
  // lease
  private LockType leaseLock = null;
  private Object leaseParam = null;
  private List<Lease> leaseResult = null;
  // lease paths
  private LockType lpLock = null;
  private Object lpParam = null;
  // replica
  private LockType replicaLock = null;
  private Object replicaParam = null;
  // corrupt
  private LockType crLock = null;
  private Object crParam = null;
  // excess
  private LockType erLock = null;
  private Object erParam = null;
  //replica under contruction
  private LockType rucLock = null;
  private Object rucParam = null;
  // under replicated blocks
  private LockType urbLock = null;
  private Object urbParam = null;
  // pending blocks
  private LockType pbLock = null;
  private Object pbParam = null;
  // invalidated blocks
  private LockType invLocks = null;
  private Object invParam = null;

  private List<Lease> acquireLeaseLock(LockType lockType, Object leaseParam) throws PersistanceException {

    setLockMode(lockType);

    ArrayList<Lease> leases = new ArrayList<Lease>();
    if (leaseParam != null) {
      String leaseHolder = (String) leaseParam;
      checkStringParam(leaseParam);
      if (inodeResult != null && inodeResult instanceof INodeFile) {
        String inodeHolder = ((INodeFile) inodeResult).getClientName();
        Lease l1 = null;
        Lease l2 = null;
        // must acquire lock with holders sorted lexicographically, descending order
        if (inodeHolder.compareTo(leaseHolder) > 0) {
          l1 = EntityManager.find(Lease.Finder.ByPKey, inodeHolder);
          l2 = EntityManager.find(Lease.Finder.ByPKey, leaseHolder);
        } else {
          l1 = EntityManager.find(Lease.Finder.ByPKey, leaseHolder);
          l2 = EntityManager.find(Lease.Finder.ByPKey, inodeHolder);
        }

        if (l1 != null) {
          leases.add(l1);
        }

        if (l2 != null) {
          leases.add(l2);
        }

        return leases;
      }

      Lease l = EntityManager.find(Lease.Finder.ByPKey, leaseHolder);
      if (l != null) {
        leases.add(l);
      }

      return leases;
    }

    if (inodeResult != null && inodeResult instanceof INodeFile) {
      String inodeHolder = ((INodeFile) inodeResult).getClientName();
      Lease l = EntityManager.find(Lease.Finder.ByPKey, inodeHolder);
      if (l != null) {
        leases.add(l);
      }
    }

    return leases;
  }

  private void checkStringParam(Object param) {
    if (param != null && !(param instanceof String)) {
      throw new IllegalArgumentException("Param is expected to be a String but is " + param.getClass().getName());
    }
  }

  private void acquireLeasePathsLock(LockType lockType) throws PersistanceException {
    setLockMode(lockType);
    if (leaseResult != null && !leaseResult.isEmpty()) {
      for (Lease l : leaseResult) {
        // We don't need to keep the results, just cache them
        EntityManager.findList(LeasePath.Finder.ByHolderId, l.getHolderID());
      }
    }
  }

  private void acquireReplicasLock(LockType lockType, FinderType finder) throws PersistanceException {
    setLockMode(lockType);
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        EntityManager.findList(finder, b.getBlockId());
      }
    }
  }

  private void acquireBlockRelatedLock(LockType lockType, FinderType finder) throws PersistanceException {
    setLockMode(lockType);
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        EntityManager.find(finder, b.getBlockId());
      }
    }
  }

  public enum LockType {

    WRITE, READ
  }

  public enum INodeLockType {

    BY_PATH_ALL_READ_LOCK // read lock on all the paths components
    , BY_PATH_LAST_WRITE_LOCK // read lock on all path components and write lock on the last path component.
    , BY_INODE_ID_WRITE_LOCK // write lock over the given inode
    , BY_INODE_ID_READ_LOCK // read lock over the given inode
  }

  public TransactionLockAcquirer addINode(INodeLockType lock, Object param, INode rootDir) {
    this.inodeLock = lock;
    this.inodeParam = param;
    this.rootDir = rootDir;
    return this;
  }

  public TransactionLockAcquirer addBlock(LockType lock, Object param) {
    this.blockLock = lock;
    this.blockParam = param;
    return this;
  }

  public TransactionLockAcquirer addLease(LockType lock, Object param) {
    this.leaseLock = lock;
    this.leaseParam = param;
    return this;
  }

  public TransactionLockAcquirer addCorrupt(LockType lock, Object param) {
    this.crLock = lock;
    this.crParam = param;
    return this;
  }

  public TransactionLockAcquirer addExcess(LockType lock, Object param) {
    this.erLock = lock;
    this.erParam = param;
    return this;
  }

  public TransactionLockAcquirer addReplicaUc(LockType lock, Object param) {
    this.rucLock = lock;
    this.rucParam = param;
    return this;
  }

  public TransactionLockAcquirer addReplica(LockType lock, Object param) {
    this.replicaLock = lock;
    this.replicaParam = param;
    return this;
  }

  public TransactionLockAcquirer addLeasePath(LockType lock, Object param) {
    this.lpLock = lock;
    this.lpParam = param;
    return this;
  }

  public TransactionLockAcquirer addUnderReplicatedBlock(LockType lock, Object param) {
    this.urbLock = lock;
    this.urbParam = param;
    return this;
  }
  
  public TransactionLockAcquirer addInvalidatedBlock(LockType lock, Object param) {
    this.invLocks = lock;
    this.invParam = param;
    return this;
  }

  public void acquire() throws PersistanceException {
    // acuires lock in order
    if (inodeLock != null && inodeParam != null) {
      inodeResult = acquireInodeLocks(inodeLock, inodeParam);
    }

    if (blockLock != null) {
      if (inodeLock != null && blockParam != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }

      blockResults = acquireBlockLock(blockLock, blockParam);
    }

    if (leaseLock != null) {
      leaseResult = acquireLeaseLock(leaseLock, leaseParam);
    }

    if (lpLock != null) {
      acquireLeasePathsLock(lpLock);
    }

    if (blockResults != null && !blockResults.isEmpty()) {
      if (replicaLock != null) {
        acquireReplicasLock(replicaLock, IndexedReplica.Finder.ByBlockId);
      }

      if (crLock != null) {
        acquireReplicasLock(crLock, CorruptReplica.Finder.ByBlockId);
      }

      if (erLock != null) {
        acquireReplicasLock(erLock, ExcessReplica.Finder.ByBlockId);
      }

      if (rucLock != null) {
        acquireReplicasLock(rucLock, ReplicaUnderConstruction.Finder.ByBlockId);
      }

      if (invLocks != null) {
        // FIXME
        throw new UnsupportedOperationException("Invalidated Blocks are not supported yet");
      }

      if (urbLock != null) {
        acquireBlockRelatedLock(urbLock, UnderReplicatedBlock.Finder.ByBlockId);
      }

      if (pbLock != null) {
        acquireBlockRelatedLock(pbLock, PendingBlockInfo.Finder.ByPKey);
      }
    }
  }

  private INode acquireInodeLocks(INodeLockType lock, Object param) {
    try {
      switch (lock) {
        case BY_PATH_ALL_READ_LOCK:
          return acquireInodeLockByPath(false, param);
        case BY_PATH_LAST_WRITE_LOCK:
          return acquireInodeLockByPath(true, param);
        case BY_INODE_ID_READ_LOCK:
          break;
        case BY_INODE_ID_WRITE_LOCK:
          break;
        default:
          throw new IllegalArgumentException("Unknown type " + lock.name());
      }
    } catch (UnresolvedPathException ex) {
      // FIXME throw exception to the upper level
      Logger.getLogger(TransactionLockAcquirer.class.getName()).log(Level.SEVERE, null, ex);
    } catch (PersistanceException ex) {
      Logger.getLogger(TransactionLockAcquirer.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  private INode acquireInodeLockByPath(boolean lastWriteLock, Object path) throws UnresolvedPathException, PersistanceException {
    boolean resolveLink = true; // FIXME [H]: This can differ for different operations

    checkStringParam(path);

    byte[][] components = INode.getPathComponents((String) path);
    INode curNode = rootDir;

    assert INode.compareBytes(curNode.getNameBytes(), components[0]) == 0 :
            "Incorrect name " + curNode.getName() + " expected "
            + DFSUtil.bytes2String(components[0]);

    int count = 0;
    boolean lastComp = (count == components.length - 1);
    if (lastComp && lastWriteLock) // if root is the last directory, we should acquire the write lock over the root
    {
      EntityManager.writeLock();
      curNode = EntityManager.find(INode.Finder.ByPKey, 0);
      return curNode;
    }

    while (count < components.length && curNode != null) {

      if (lastWriteLock && (count + 1 == components.length - 1)) {
        EntityManager.writeLock(); // if the next p-component is the last one, acquire the write lock
      } else {
        EntityManager.readLock();
      }

      curNode = getChildINode(components[count + 1], curNode.getId());
      count++;
      lastComp = (count == components.length - 1);

      if (curNode.isLink() && (!lastComp || (lastComp && resolveLink))) {
        final String symPath = constructPath(components, 0, components.length);
        final String preceding = constructPath(components, 0, count);
        final String remainder =
                constructPath(components, count + 1, components.length);
        final String link = DFSUtil.bytes2String(components[count]);
        final String target = ((INodeSymlink) curNode).getLinkValue();
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("UnresolvedPathException "
                  + " path: " + symPath + " preceding: " + preceding
                  + " count: " + count + " link: " + link + " target: " + target
                  + " remainder: " + remainder);
        }
        throw new UnresolvedPathException(symPath, preceding, remainder, target);
      }
      if (lastComp || !curNode.isDirectory()) {
        break;
      }
    }

    return curNode;
  }

  private INode getChildINode(byte[] name, long parentId) throws PersistanceException {
    return EntityManager.find(INode.Finder.ByNameAndParentId,
            DFSUtil.bytes2String(name), parentId);
  }

  private static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  private List<BlockInfo> acquireBlockLock(LockType lock, Object param) throws PersistanceException {

    if (param != null && !(param instanceof Long)) {
      throw new IllegalArgumentException("Param is expected to be Long but received " + param.getClass().toString());
    }

    List<BlockInfo> blocks = new ArrayList<BlockInfo>();

    if (blockParam != null) {
      long bid = (Long) param;
      setLockMode(lock);
      BlockInfo result = EntityManager.find(BlockInfo.Finder.ById, bid);
      if (result != null) {
        blocks.add(result);
      }
    } else if (inodeResult != null && inodeResult instanceof INodeFile) {
      setLockMode(lock);
      blocks.addAll(EntityManager.findList(BlockInfo.Finder.ByInodeId, inodeResult.getId()));
    }

    return blocks;
  }

  private void setLockMode(LockType mode) {
    if (mode == LockType.WRITE) {
      EntityManager.writeLock();
    } else {
      EntityManager.readLock();
    }
  }
}
