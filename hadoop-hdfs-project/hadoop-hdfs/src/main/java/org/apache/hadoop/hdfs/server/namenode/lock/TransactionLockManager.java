package org.apache.hadoop.hdfs.server.namenode.lock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.GenerationStamp;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Leader;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TransactionLockManager {

  private final static Log LOG = LogFactory.getLog(TransactionLockManager.class);
  //inode
  private INodeLockType inodeLock = null;
  private INodeResolveType inodeResolveType = null;
  private String[] inodeParam = null;
  private INode[] inodeResult = null;
  private boolean resolveLink = true; // the file is a symlink should it resolve it?
  protected LinkedList<INode> resolvedInodes = null; // For the operations requires to have inodes before starting transactions.
  //block
  private LockType blockLock = null;
  private Long blockParam = null;
  private Collection<BlockInfo> blockResults = null;
  // lease
  private LockType leaseLock = null;
  private String leaseParam = null;
  private Collection<Lease> leaseResults = null;
  private LockType nnLeaseLock = null; // acquire lease for Name-node
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
  // block token key
  private LockType blockKeyLock = null;
  private List<Integer> blockKeyIds = null;
  private List<Short> blockKeyTypes = null;
  // block generation stamp
  private LockType generationStampLock = null;
  // Leader
  private LockType leaderLock = null;
  private long[] leaderIds = null;

  public TransactionLockManager() {
  }

  public TransactionLockManager(LinkedList<INode> resolvedInodes) {
    this.resolvedInodes = resolvedInodes;
  }

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

  /**
   * Acquires lock on lease path and lease having leasepath. This is used by the
   * test cases.
   *
   * @param leasePath
   */
  public void acquireByLeasePath(String leasePath, LockType leasePathLock, LockType leaseLock) throws PersistanceException {
    LeasePath lp = TransactionLockAcquirer.acquireLock(leasePathLock, LeasePath.Finder.ByPKey, leasePath);
    if (lp != null) {
      TransactionLockAcquirer.acquireLock(leaseLock, Lease.Finder.ByHolderId, lp.getHolderId());
    }
  }

  private void checkStringParam(Object param) {
    if (param != null && !(param instanceof String)) {
      throw new IllegalArgumentException("Param is expected to be a String but is " + param.getClass().getName());
    }
  }

  private List<LeasePath> acquireLeasePathsLock(LockType lock) throws PersistanceException {
    List<LeasePath> lPaths = new LinkedList<LeasePath>();
    if (leaseResults != null) {
      for (Lease l : leaseResults) {
        Collection<LeasePath> result = TransactionLockAcquirer.acquireLockList(lock, LeasePath.Finder.ByHolderId, l.getHolderID());
        if (!l.getHolder().equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) { // We don't need to keep the lps result for namenode-lease. 
          lPaths.addAll(result);
        }
      }
    }

    return lPaths;
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
        } else {
          // immediate children of  INodeFile is the inode itself.
          children.add(dir);
        }
      }
    }
    //if(child)
    inodes = new INode[children.size()];
    return children.toArray(inodes);
  }

  private INode[] findChildrenRecursively(INode[] inodes) throws PersistanceException {
    ArrayList<INode> children = new ArrayList<INode>();
    LinkedList<INode> unCheckedDirs = new LinkedList<INode>();
    if (inodes != null) {
      for (INode inode : inodes) {
        if (inode instanceof INodeDirectory) {
          unCheckedDirs.add(inode);
        } else {
          children.add(inode);
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

  private String buildPath(String path, int size) {
    StringBuilder builder = new StringBuilder();
    byte[][] components = INode.getPathComponents(path);
    String delimiter = "/";
    if (components.length <= 1) {
      return delimiter; // return only root
    }    // build path for number of existing path components plus one
    for (int i = 1; i <= Math.min(components.length - 1, size + 1); i++) {
      builder.append(delimiter);
      builder.append(DFSUtil.bytes2String(components[i]));
    }

    return builder.toString();
  }

  private Lease acquireNameNodeLease() throws PersistanceException {
    if (nnLeaseLock != null) {
      return TransactionLockAcquirer.acquireLock(nnLeaseLock, Lease.Finder.ByPKey, HdfsServerConstants.NAMENODE_LEASE_HOLDER);
    }
    return null;
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
    , FROM_CHILD_TO_ROOT // resolves inode by having an inode as a child through the root
  }

  public TransactionLockManager addINode(INodeResolveType resolveType,
          INodeLockType lock, boolean resolveLink, String[] param) {
    this.inodeLock = lock;
    this.inodeResolveType = resolveType;
    this.inodeParam = param;
    this.resolveLink = resolveLink;
    return this;
  }

  public TransactionLockManager addINode(INodeResolveType resolveType,
          INodeLockType lock, String[] param) {
    return addINode(resolveType, lock, true, param);
  }

  public TransactionLockManager addINode(INodeLockType lock) {
    addINode(null, lock, null);
    return this;
  }

  public TransactionLockManager addINode(INodeResolveType resolveType, INodeLockType lock) {
    return addINode(resolveType, lock, true, null);
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

  public TransactionLockManager addNameNodeLease(LockType lock) {
    this.nnLeaseLock = lock;
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

  public TransactionLockManager addGenerationStamp(LockType lock) {
    this.generationStampLock = lock;
    return this;
  }

  /**
   * Lock on block token key data.
   *
   * @param lock
   * @param keyId
   * @return
   */
  public TransactionLockManager addBlockKeyLockById(LockType lock, int keyId) {
    blockKeyLock = lock;
    if (blockKeyIds == null) {
      blockKeyIds = new ArrayList<Integer>();
    }
    blockKeyIds.add(keyId);
    return this;
  }

  public TransactionLockManager addBlockKeyLockByType(LockType lock, short type) {
    blockKeyLock = lock;
    if (blockKeyTypes == null) {
      blockKeyTypes = new ArrayList<Short>();
    }
    blockKeyTypes.add(type);
    return this;
  }

  public TransactionLockManager addLeaderLock(LockType lock, long... ids) {
    this.leaderLock = lock;
    this.leaderIds = ids;
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

    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
    acquireLeaderLock();
  }

  public void acquireForRename() throws PersistanceException, UnresolvedPathException {
    acquireForRename(false);
  }

  public void acquireForRename(boolean allowExistingDir) throws PersistanceException, UnresolvedPathException {
    // TODO[H]: Sort before taking locks.
    // acuires lock in order
    String src = inodeParam[0];
    String dst = inodeParam[1];
    if (inodeLock != null && inodeParam != null && inodeParam.length > 0) {
      INode[] inodeResult1 = acquireInodeLocks(inodeResolveType, inodeLock, src);
      INode[] inodeResult2 = acquireInodeLocks(inodeResolveType, inodeLock, dst);
      if (allowExistingDir) // In deprecated rename, it allows to move a dir to an existing destination.
      {
        LinkedList<INode> dstINodes = TransactionLockAcquirer.acquireInodeLockByPath(inodeLock, dst, resolveLink); // reads from snapshot.
        byte[][] dstComponents = INode.getPathComponents(dst);
        byte[][] srcComponents = INode.getPathComponents(src);
        if (dstINodes.size() == dstComponents.length - 1 && dstINodes.getLast().isDirectory()) {
          //the dst exist and is a directory.
          INode existingInode = TransactionLockAcquirer.acquireINodeLockByNameAndParentId(
                  inodeLock,
                  DFSUtil.bytes2String(srcComponents[srcComponents.length - 1]),
                  dstINodes.getLast().getId());
//        inodeResult = new INode[inodeResult1.length + inodeResult2.length + 1];
//        if (existingInode != null & !existingInode.isDirectory()) {
//          inodeResult[inodeResult.length - 1] = existingInode;
//        }
        }
      }
      inodeResult = new INode[inodeResult1.length + inodeResult2.length];
      System.arraycopy(inodeResult1, 0, inodeResult, 0, inodeResult1.length);
      System.arraycopy(inodeResult2, 0, inodeResult, inodeResult1.length, inodeResult2.length);
    }

    if (blockLock != null) {
      if (inodeLock != null && blockParam != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }

      blockResults = acquireBlockLock(blockLock, blockParam);
    }

    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
    acquireLeaderLock();
  }

  private void acquireLeaderLock() throws PersistanceException {
    if (leaderLock != null) {
      if (leaderIds.length == 0) {
        TransactionLockAcquirer.acquireLockList(leaderLock, Leader.Finder.All);
      } else {
        for (long id : leaderIds) {
          TransactionLockAcquirer.acquireLock(
                  leaderLock,
                  Leader.Finder.ById,
                  id,
                  Leader.DEFAULT_PARTITION_VALUE);
        }
      }
    }
  }

  private void acquireLeaseAndLpathLockNormal() throws PersistanceException {
    if (leaseLock != null) {
      leaseResults = acquireLeaseLock(leaseLock, leaseParam);
    }

    if (lpLock != null) {
      acquireLeasePathsLock(lpLock);
    }
  }

  /**
   * Acquires lock on the lease, lease-path, replicas, excess, corrupt,
   * invalidated, under-replicated and pending blocks.
   *
   * @throws PersistanceException
   */
  private void acquireBlockRelatedLocksNormal() throws PersistanceException {

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

    if (blockKeyLock != null) {
      if (blockKeyIds != null) {
        for (int id : blockKeyIds) {
          TransactionLockAcquirer.acquireLock(blockKeyLock, BlockKey.Finder.ById, id);
        }
      }
      if (blockKeyTypes != null) {
        for (short type : blockKeyTypes) {
          TransactionLockAcquirer.acquireLock(blockKeyLock, BlockKey.Finder.ByType, type);
        }
      }
    }

    if (generationStampLock != null) {
      TransactionLockAcquirer.acquireLock(generationStampLock, GenerationStamp.Finder.Counter);
    }
  }

  private INode[] acquireInodeLocks(INodeResolveType resType, INodeLockType lock, String... params) throws UnresolvedPathException, PersistanceException {
    INode[] inodes = new INode[params.length];
    switch (resType) {
      case ONLY_PATH: // Only use memcached for this case.
      case PATH_AND_IMMEDIATE_CHILDREN: // Memcached not applicable for delete of a dir (and its children)
      case PATH_AND_ALL_CHILDREN_RECURESIVELY:
        for (int i = 0; i < params.length; i++) {
          // TODO - MemcacheD Lookup of path
          // On
          LinkedList<INode> resolvedInodes =
                  TransactionLockAcquirer.acquireInodeLockByPath(lock, params[i], resolveLink);
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
      // e.g. mkdir -d /opt/long/path which creates subdirs.
      // That is, the HEAD and some ancestor inodes might not exist yet.
      case ONLY_PATH_WITH_UNKNOWN_HEAD: // Can try and use memcached for this case.
        for (int i = 0; i < params.length; i++) {
          // TODO Test this in all different possible scenarios
          String fullPath = params[i];
          checkPathIsResolved();
          int resolvedSize = resolvedInodes.size();
          String existingPath = buildPath(fullPath, resolvedSize);
          resolvedInodes = TransactionLockAcquirer.acquireInodeLockByPath(lock, existingPath, resolveLink);
          if (resolvedSize <= resolvedInodes.size()) { // FIXME: Due to removing a dir, this could become false. So we may retry. Anyway, it can be livelock-prone
            // lock any remained path component if added between the two transactions
            INode baseDir = resolvedInodes.peekLast();
            LinkedList<INode> rest = TransactionLockAcquirer.acquireLockOnRestOfPath(lock, baseDir,
                    fullPath, existingPath, resolveLink);
            resolvedInodes.addAll(rest);
            inodes[i] = resolvedInodes.peekLast();
          }
        }
        break;

      default:
        throw new IllegalArgumentException("Unknown type " + lock.name());
    }

    return inodes;
  }

  private void checkPathIsResolved() throws INodeResolveException {
    if (resolvedInodes == null) {
      throw new INodeResolveException(String.format(
              "Requires to have inode-id(s) in order to do this operation. "
              + "ResolvedInodes is null."));
    }
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
   * This method acquires lockk on the inode starting with a block-id. The
   * lock-types should be set before using add* methods. Otherwise, no lock
   * would be acquired.
   *
   * @throws PersistanceException
   */
  public void acquireByBlock(long inodeId) throws PersistanceException, UnresolvedPathException {
    if (inodeLock == null || blockParam == null) // inodelock must be set before.
    {
      return;
    }
    INode inode = TransactionLockAcquirer.acquireINodeLockById(inodeLock, inodeId);
    if (this.inodeResolveType == INodeResolveType.FROM_CHILD_TO_ROOT) {
      checkPathIsResolved();
      if (!resolvedInodes.isEmpty()) {
        takeLocksFromRootToLeaf(resolvedInodes, inodeLock);
      }
    }

    if (inode != null) {
      if (!(inode instanceof INodeFile)) {
        return; //TODO: it should abort the transaction and retry at this stage. Cause something is changed in the storage.
      }
      inodeResult = new INode[1];
      inodeResult[0] = inode;
    }

    blockResults = TransactionLockAcquirer.acquireLockList(
            blockLock,
            BlockInfo.Finder.ByInodeId,
            inodeId);

    if (blockResults.isEmpty()) {
      BlockInfo block = TransactionLockAcquirer.acquireLock(
              blockLock,
              BlockInfo.Finder.ById,
              blockParam);
      if (block != null) {
        blockResults.add(block);
      }
    }

    //TODO: it should abort the transaction and retry at this stage. Cause something is changed in the storage.
    int i = 0;
    for (Block block : blockResults) {
      if (block.getBlockId() == blockParam.longValue()) {
        break;
      }
      i++;
    }
    if (i == blockResults.size()) {
      return; // The state of the inode->blocks is inconsistent, retry.
    }

    // read-committed block is the same as block found by inode-file so everything is fine and continue the rest.
    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();

  }

  private void takeLocksFromRootToLeaf(LinkedList<INode> inodes, INodeLockType inodeLock) throws PersistanceException {

    StringBuilder msg = new StringBuilder();
    msg.append("Took Lock on the entire path ");
    for (int i = 0; i < inodes.size(); i++) {
      INode lockedINode;
      if (i == (inodes.size() - 1)) // take specified lock
      {
        lockedINode = TransactionLockAcquirer.acquireINodeLockById(inodeLock, inodes.get(i).getId());
      } else // take read commited lock
      {
        lockedINode = TransactionLockAcquirer.acquireINodeLockById(INodeLockType.READ_COMMITED, inodes.get(i).getId());
      }

      if (!lockedINode.getName().equals("")) {
        msg.append("/");
        msg.append(lockedINode.getName());
      }


    }
    LOG.debug(msg.toString());
  }

  public void acquireByLease(SortedSet<String> sortedPaths) throws PersistanceException, UnresolvedPathException {
    if (leaseParam == null) {
      return;
    }

    inodeResult = acquireInodeLocks(
            INodeResolveType.ONLY_PATH,
            inodeLock,
            sortedPaths.toArray(new String[sortedPaths.size()]));

    if (inodeResult.length == 0) {
      return; // TODO: something is wrong, it should retry again.
    }
    leaseResults = new ArrayList<Lease>();
    Lease nnLease = acquireNameNodeLease(); // NameNode lease is always acquired first.
    if (nnLease != null) {
      leaseResults.add(nnLease);
    }
    Lease lease = TransactionLockAcquirer.acquireLock(leaseLock, Lease.Finder.ByPKey, leaseParam);
    if (lease == null) {
      return; // Lease does not exist anymore.
    }
    leaseResults.add(lease);
    blockResults = acquireBlockLock(blockLock, null);

    List<LeasePath> lpResults = acquireLeasePathsLock(lpLock);
    if (lpResults.size() > sortedPaths.size()) {
      return; // TODO: It should retry again, cause there are new lease-paths for this lease which we have not acquired their inodes locks.
    }

    acquireBlockRelatedLocksNormal();
  }
}
