package org.apache.hadoop.hdfs.server.namenode.lock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TransactionLockManager {

  //inode
  private INodeLockType inodeLock = null;
  private INodeResolveType inodeResolveType = null;
  private String[] inodeParam = null;
  private INode[] inodeResult = null;
  private INode rootDir = null;
  //block
  private LockType blockLock = null;
  private Long blockParam = null;
  private Collection<BlockInfo> blockResults = null;
  // lease
  private LockType leaseLock = null;
  private String leaseParam = null;
  private Collection<Lease> leaseResult = null;
  // lease paths
  private LockType lpLock = null;
  // replica
  private LockType replicaLock = null;
  // corrupt
  private LockType crLock = null;
  // excess
  private LockType erLock = null;
  //replica under contruction
  private LockType rucLock = null;
  // under replicated blocks
  private LockType urbLock = null;
  // pending blocks
  private LockType pbLock = null;
  // invalidated blocks
  private LockType invLocks = null;

  private List<Lease> acquireLeaseLock(LockType lock, String holder) throws PersistanceException {

    checkStringParam(holder);
    SortedSet<String> holders = new TreeSet<String>();
    if (holder != null) {
      holders.add((String) holder);
    }

    if (inodeResult != null) {
      for (INode f : inodeResult) {
        if (f instanceof INodeFile) {
          holders.add(((INodeFile) f).getClientName());
        }
      }
    }

    List<Lease> leases = new ArrayList<Lease>();
    for (String h : holders) {
      Lease lease = TransactionLockAcquirer.acquireLock(lock, Lease.Finder.ByPKey, h);
      if (lease != null) {
        leases.add(lease);
      }
    }

    return leases;
  }

  private void checkStringParam(Object param) {
    if (param != null && !(param instanceof String)) {
      throw new IllegalArgumentException("Param is expected to be a String but is " + param.getClass().getName());
    }
  }

  private void acquireLeasePathsLock(LockType lock) throws PersistanceException {
    if (leaseResult != null && !leaseResult.isEmpty()) {
      for (Lease l : leaseResult) {
        // We don't need to keep the results, just cache them
        TransactionLockAcquirer.acquireLockList(lock, LeasePath.Finder.ByHolderId, l.getHolderID());
      }
    }
  }

  private void acquireReplicasLock(LockType lock, FinderType finder) throws PersistanceException {
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        TransactionLockAcquirer.acquireLockList(lock, finder, b.getBlockId());
      }
    }
  }

  private void acquireBlockRelatedLock(LockType lock, FinderType finder) throws PersistanceException {
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        TransactionLockAcquirer.acquireLock(lock, finder, b.getBlockId());
      }
    }
  }

  private INode[] findImmediateChildren(INode[] inodes) throws PersistanceException {
    ArrayList<INode> children = new ArrayList<INode>();
    if (inodes != null) {
      for (INode dir : inodes) {
        if (dir instanceof INodeDirectory) {
          children.addAll(((INodeDirectory) dir).getChildren());
        }
      }
    }
    inodes = new INode[children.size()];
    return children.toArray(inodes);
  }

  private INode[] findChildrenRecursively(INode[] inodes) throws PersistanceException {
    ArrayList<INode> children = new ArrayList<INode>();
    LinkedList<INode> unCheckedDirs = new LinkedList<INode>();
    if (inodes != null) {
      for (INode dir : inodes) {
        if (dir instanceof INodeDirectory) {
          unCheckedDirs.add(dir);
        }
      }
    }

    // Find all the children in the sub-directories.
    while (!unCheckedDirs.isEmpty()) {
      INode next = unCheckedDirs.poll();
      if (next instanceof INodeDirectory) {
        unCheckedDirs.addAll(((INodeDirectory) next).getChildren());
      } else if (next instanceof INodeFile) {
        children.add(next); // We only need INodeFiles later to get the blocks.
      }
    }

    inodes = new INode[children.size()];
    return children.toArray(inodes);
  }

  private String buildPath(LinkedList<INode> resolvedInodes) {
    StringBuilder path = new StringBuilder();
    String delimiter = "/";
    path.append(delimiter);
    for (INode inode : resolvedInodes) {
      path.append(inode.getName()).append(delimiter);
    }

    return path.toString();
  }

  public enum LockType {

    WRITE, READ, READ_COMMITTED
  }

  public enum INodeLockType {

    WRITE,
    WRITE_ON_PARENT // Write lock on the parent of the last path component. This has the WRITE effect when using inode-id.
    , READ, READ_COMMITED // No Lock
  }

  public enum INodeResolveType {

    ONLY_PATH // resolve only the given path
    , ONLY_PATH_WITH_UNKNOWN_HEAD // resolve a path which some of its path components might not exist
    , PATH_AND_IMMEDIATE_CHILDREN // resolve path and find the given directory's children
    , PATH_AND_ALL_CHILDREN_RECURESIVELY // resolve the given path and find all the children recursively.
  }

  public TransactionLockManager addINode(INodeResolveType resolveType,
          INodeLockType lock, String[] param, INode rootDir) {
    this.inodeLock = lock;
    this.inodeResolveType = resolveType;
    this.inodeParam = param;
    this.rootDir = rootDir;
    return this;
  }

  public TransactionLockManager addINode(INodeLockType lock) {
    addINode(null, lock, null, null);
    return this;
  }

  public TransactionLockManager addBlock(LockType lock, Long param) {
    this.blockLock = lock;
    this.blockParam = param;
    return this;
  }

  public TransactionLockManager addBlock(LockType lock) {
    addBlock(lock, null);
    return this;
  }

  public TransactionLockManager addLease(LockType lock, String param) {
    this.leaseLock = lock;
    this.leaseParam = param;
    return this;
  }

  public TransactionLockManager addLease(LockType lock) {
    addLease(lock, null);
    return this;
  }

  public TransactionLockManager addCorrupt(LockType lock) {
    this.crLock = lock;
    return this;
  }

  public TransactionLockManager addExcess(LockType lock) {
    this.erLock = lock;
    return this;
  }

  public TransactionLockManager addReplicaUc(LockType lock) {
    this.rucLock = lock;
    return this;
  }

  public TransactionLockManager addReplica(LockType lock) {
    this.replicaLock = lock;
    return this;
  }

  public TransactionLockManager addLeasePath(LockType lock) {
    this.lpLock = lock;
    return this;
  }

  public TransactionLockManager addUnderReplicatedBlock(LockType lock) {
    this.urbLock = lock;
    return this;
  }

  public TransactionLockManager addInvalidatedBlock(LockType lock) {
    this.invLocks = lock;
    return this;
  }

  public TransactionLockManager addPendingBlock(LockType lock) {
    this.pbLock = lock;
    return this;
  }

  public void acquire() throws PersistanceException, UnresolvedPathException {
    // acuires lock in order
    if (inodeLock != null && inodeParam != null && inodeParam.length > 0) {
      inodeResult = acquireInodeLocks(inodeResolveType, inodeLock, inodeParam);
    }

    if (blockLock != null) {
      if (inodeLock != null && blockParam != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }

      blockResults = acquireBlockLock(blockLock, blockParam);
    }

    acquireLockInternal(); // acquire locks on the rest of the tables.
  }

  /**
   * Acquires lock on the lease, lease-path, replicas, excess, corrupt, invalidated,
   * under-replicated and pending blocks.
   * @throws PersistanceException 
   */
  private void acquireLockInternal() throws PersistanceException {
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
        acquireReplicasLock(invLocks, InvalidatedBlock.Finder.ByBlockId);
      }

      if (urbLock != null) {
        acquireBlockRelatedLock(urbLock, UnderReplicatedBlock.Finder.ByBlockId);
      }

      if (pbLock != null) {
        acquireBlockRelatedLock(pbLock, PendingBlockInfo.Finder.ByPKey);
      }
    }
  }

  private INode[] acquireInodeLocks(INodeResolveType resType, INodeLockType lock, String... params) throws UnresolvedPathException, PersistanceException {
    INode[] inodes = new INode[params.length];
    switch (resType) {
      case ONLY_PATH:
      case PATH_AND_IMMEDIATE_CHILDREN:
      case PATH_AND_ALL_CHILDREN_RECURESIVELY:
        for (int i = 0; i < params.length; i++) {
          LinkedList<INode> resolvedInodes = TransactionLockAcquirer.acquireInodeLockByPath(lock, params[i], rootDir);
          if (resolvedInodes.size() > 0) {
            inodes[i] = resolvedInodes.peekLast();
          }
        }
        if (resType == INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
          inodes = findImmediateChildren(inodes);
        } else if (resType == INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY) {
          inodes = findChildrenRecursively(inodes);
        }
        break;
      case ONLY_PATH_WITH_UNKNOWN_HEAD:
        for (int i = 0; i < params.length; i++) {
          // supports for only one given path
          LinkedList<INode> resolvedInodes = TransactionLockAcquirer.acquireInodeLockByPath(INodeLockType.READ_COMMITED, params[0], rootDir);
          int resolvedSize = resolvedInodes.size();
          String existingPath = buildPath(resolvedInodes);
          EntityManager.clearContext(); // clear the context, so it won't use in-memory data.
          resolvedInodes = TransactionLockAcquirer.acquireInodeLockByPath(lock, existingPath, rootDir);
          if (resolvedSize == resolvedInodes.size()) { // FIXME: Due to removing a dir, this could become false. So we may retry. Anyway, it can be livelock-prone
            inodes[i] = resolvedInodes.getLast();
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + lock.name());
    }

    return inodes;
  }

  private List<BlockInfo> acquireBlockLock(LockType lock, Long param) throws PersistanceException {

    List<BlockInfo> blocks = new ArrayList<BlockInfo>();

    if (blockParam != null) {
      long bid = (Long) param;

      BlockInfo result = TransactionLockAcquirer.acquireLock(lock, BlockInfo.Finder.ById, bid);
      if (result != null) {
        blocks.add(result);
      }
    } else if (inodeResult != null) {
      for (INode inode : inodeResult) {
        if (inode instanceof INodeFile) {
          blocks.addAll(TransactionLockAcquirer.acquireLockList(lock, BlockInfo.Finder.ByInodeId, inode.getId()));
        }
      }
    }

    return blocks;
  }

  /**
   * This method acquires lockk on the inode starting with a block-id. The lock-types
   * should be set before using add* methods. Otherwise, no lock would be acquired.
   * @throws PersistanceException 
   */
  public void acquireByBlock() throws PersistanceException, UnresolvedPathException {
    if (inodeLock == null) // inodelock must be set before.
    {
      return;
    }
    if (blockParam == null) {
      return;
    }

    EntityManager.readCommited();
    BlockInfo rcBlock = EntityManager.find(BlockInfo.Finder.ById, blockParam);

    if (rcBlock == null) {
      return;
    }
    EntityManager.clearContext();

    INode inode = TransactionLockAcquirer.acquireINodeLockById(inodeLock, rcBlock.getInodeId());

    //TODO: it should abort the transaction and retry at this stage. Cause something is changed in the storage.
    if (inode == null || !(inode instanceof INodeFile)) {
      return;
    }
    inodeResult = new INode[1];
    inodeResult[0] = inode;

    blockResults = TransactionLockAcquirer.acquireLockList(blockLock,
            BlockInfo.Finder.ByInodeId, ((INodeFile) inode).getId());

    //TODO: it should abort the transaction and retry at this stage. Cause something is changed in the storage.
    if (blockResults.isEmpty() || !blockResults.contains(rcBlock)) {
      return;
    }

    // read-committed block is the same as block found by inode-file so everything is fine and continue the rest.
    acquireLockInternal();

  }
}
