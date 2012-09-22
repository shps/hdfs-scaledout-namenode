package org.apache.hadoop.hdfs.server.namenode.lock;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager.INodeLockType;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager.LockType;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TransactionLockAcquirer {

  public static ConcurrentHashMap<String, ReentrantLock> datanodeLocks = new ConcurrentHashMap<String, ReentrantLock>();

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

  public static <T> Collection<T> acquireLockList(TransactionLockManager.LockType lock, FinderType<T> finder, Object param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return EntityManager.findList(finder);
    } else {
      return EntityManager.findList(finder, param);
    }
  }

  public static <T> T acquireLock(TransactionLockManager.LockType lock, FinderType<T> finder, Object param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return null;
    }
    return EntityManager.find(finder, param);
  }

  public static LinkedList<INode> acquireInodeLockByPath(INodeLockType lock, String path, INode rootDir) throws UnresolvedPathException, PersistanceException {
    LinkedList<INode> resolvedInodes = new LinkedList<INode>();
    boolean resolveLink = true; // FIXME [H]: This can differ for different operations

    if (path == null) {
      return resolvedInodes;
    }

    byte[][] components = INode.getPathComponents(path);
    INode curNode = rootDir;

    assert INode.compareBytes(curNode.getNameBytes(), components[0]) == 0 :
            "Incorrect name " + curNode.getName() + " expected "
            + DFSUtil.bytes2String(components[0]);

    int count = 0;
    boolean lastComp = (count == components.length - 1);
    if (lastComp && (lock == INodeLockType.WRITE || lock == INodeLockType.WRITE_ON_PARENT)) // if root is the last directory, we should acquire the write lock over the root
    {
      resolvedInodes.add(acquireWriteLockOnRoot());
      return resolvedInodes;
    } else if ((count == components.length - 2) && lock == INodeLockType.WRITE_ON_PARENT) // if Root is the parent
    {
      curNode = acquireWriteLockOnRoot();
    }

    while (count < components.length && curNode != null) {

      if (((lock == INodeLockType.WRITE || lock == INodeLockType.WRITE_ON_PARENT) && (count + 1 == components.length - 1))
              || (lock == INodeLockType.WRITE_ON_PARENT && (count + 1 == components.length - 2))) {
        EntityManager.writeLock(); // if the next p-component is the last one or is the parent (in case of write on parent), acquire the write lock
      } else if (lock == INodeLockType.READ_COMMITED) {
        EntityManager.readCommited();
      } else {
        EntityManager.readLock();
      }

      curNode = getChildINode(components[count + 1], curNode.getId());
      if (curNode != null) {
        resolvedInodes.add(curNode);
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
    }

    return resolvedInodes;
  }

  public static INode acquireINodeLockById(INodeLockType lock, long id) throws PersistanceException {
    lockINode(lock);
    return EntityManager.find(INode.Finder.ByPKey, id);
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

  private static INode acquireWriteLockOnRoot() throws PersistanceException {
    EntityManager.writeLock();
    return EntityManager.find(INode.Finder.ByPKey, 0);
  }

  private static INode getChildINode(byte[] name, long parentId) throws PersistanceException {
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
