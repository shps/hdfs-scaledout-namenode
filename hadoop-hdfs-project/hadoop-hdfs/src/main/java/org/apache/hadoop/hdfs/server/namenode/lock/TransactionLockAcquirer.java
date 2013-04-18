package org.apache.hadoop.hdfs.server.namenode.lock;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TransactionLockAcquirer {

  public static ConcurrentHashMap<String, ReentrantLock> datanodeLocks = new ConcurrentHashMap<String, ReentrantLock>();
  private final static Log LOG = LogFactory.getLog(TransactionLockAcquirer.class);

  public static void addToDataNodeLockMap(String storageId) {
    datanodeLocks.put(storageId, new ReentrantLock(true));
  }

  public static void removeFromDataNodeLockMap(String storageId) {
    throw new UnsupportedOperationException("removing datanodes from locks is not supported.");
  }

  public boolean lockDataNode(String storageId) {
    if (datanodeLocks.contains(this)) {
      datanodeLocks.get(storageId).lock();
      return true;
    }

    return false;
  }

  public static <T> Collection<T> acquireLockList(TransactionLockManager.LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return EntityManager.findList(finder);
    } else {
      return EntityManager.findList(finder, param);
    }
  }

  public static <T> T acquireLock(TransactionLockManager.LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return null;
    }
    return EntityManager.find(finder, param);
  }

  public static LinkedList<INode> acquireLockOnRestOfPath(INodeLockType lock, INode baseInode,
          String fullPath, String prefix, boolean resolveLink) throws PersistanceException, UnresolvedPathException {
    LinkedList<INode> resolved = new LinkedList<INode>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    int[] count = new int[]{prefixComps.length - 1};
    boolean lastComp;
    lockINode(lock);
    INode[] curInode = new INode[]{baseInode};
    while (count[0] < fullComps.length && curInode[0] != null) {
      lastComp = INodeUtil.getNextChild(
              curInode,
              fullComps,
              count,
              resolved,
              resolveLink,
              true);
      if (lastComp) {
        break;
      }
    }

    return resolved;
  }

  public static LinkedList<INode> acquireInodeLockByPath(INodeLockType lock, String path, boolean resolveLink) throws UnresolvedPathException, PersistanceException {
    LinkedList<INode> resolvedInodes = new LinkedList<INode>();

    if (path == null) {
      return resolvedInodes;
    }

    byte[][] components = INode.getPathComponents(path);
    INode[] curNode = new INode[1];

    int[] count = new int[]{0};
    boolean lastComp = (count[0] == components.length - 1);
    if (lastComp) // if root is the last directory, we should acquire the write lock over the root
    {
      resolvedInodes.add(acquireLockOnRoot(lock));
      return resolvedInodes;
    } else if ((count[0] == components.length - 2) && lock == INodeLockType.WRITE_ON_PARENT) // if Root is the parent
    {
      curNode[0] = acquireLockOnRoot(lock);
    } else {
      curNode[0] = acquireLockOnRoot(INodeLockType.READ_COMMITED);
    }

    while (count[0] < components.length && curNode[0] != null) {

      // TODO - memcached - primary key lookup for the row.
      if (((lock == INodeLockType.WRITE || lock == INodeLockType.WRITE_ON_PARENT) && (count[0] + 1 == components.length - 1))
              || (lock == INodeLockType.WRITE_ON_PARENT && (count[0] + 1 == components.length - 2))) {
        EntityManager.writeLock(); // if the next p-component is the last one or is the parent (in case of write on parent), acquire the write lock
      } else if (lock == INodeLockType.READ_COMMITED) {
        EntityManager.readCommited();
      } else {
        EntityManager.readLock();
      }

      lastComp = INodeUtil.getNextChild(
              curNode,
              components,
              count,
              resolvedInodes,
              resolveLink,
              true);
      if (lastComp) {
        break;
      }
    }

    // TODO - put invalidated cache values in memcached.

    return resolvedInodes;
  }

  // TODO - use this method when there's a hit in memcached
  // Jude's verification function
  public static INode acquireINodeLockById(INodeLockType lock, long id) throws PersistanceException {
    lockINode(lock);
    return EntityManager.find(INode.Finder.ByPKey, id);
  }

  public static INode acquireINodeLockByNameAndParentId(
          INodeLockType lock,
          String name,
          long parentId)
          throws PersistanceException {
    lockINode(lock);
    return EntityManager.find(INode.Finder.ByNameAndParentId, name, parentId);
  }

  private static void lockINode(INodeLockType lock) {
    switch (lock) {
      case WRITE:
      case WRITE_ON_PARENT:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITED:
        EntityManager.readCommited();
        break;
    }
  }

  private static INode acquireLockOnRoot(INodeLockType lock) throws PersistanceException {

    lockINode(lock);
    INode inode = EntityManager.find(INode.Finder.ByPKey, 0L);
    LOG.debug("Acquired " + lock + " on the root node");
    return inode;
  }

  private static void setLockMode(LockType mode) {
    switch (mode) {
      case WRITE:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITTED:
        EntityManager.readCommited();
        break;
    }
  }
}
