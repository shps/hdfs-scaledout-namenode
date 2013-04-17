package org.apache.hadoop.hdfs.server.namenode.lock;

import java.util.Collection;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InodeDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeasePathDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

/**
 *
 * @author hooman
 */
public class INodeUtil {

  private final static Log LOG = LogFactory.getLog(INodeUtil.class);

  // This code is based on FSDirectory code for resolving the path.
  public static boolean getNextChild(
          INode[] curInode,
          byte[][] components,
          int[] count,
          LinkedList<INode> resolvedInodes,
          boolean resolveLink,
          boolean transactional) throws UnresolvedPathException, PersistanceException {

    boolean lastComp = (count[0] == components.length - 1);
    if (curInode[0].isLink() && (!lastComp || (lastComp && resolveLink))) {
      final String symPath = constructPath(components, 0, components.length);
      final String preceding = constructPath(components, 0, count[0]);
      final String remainder =
              constructPath(components, count[0] + 1, components.length);
      final String link = DFSUtil.bytes2String(components[count[0]]);
      final String target = ((INodeSymlink) curInode[0]).getLinkValue();
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("UnresolvedPathException "
                + " path: " + symPath + " preceding: " + preceding
                + " count: " + count + " link: " + link + " target: " + target
                + " remainder: " + remainder);
      }
      throw new UnresolvedPathException(symPath, preceding, remainder, target);
    }

    if (lastComp || !curInode[0].isDirectory()) {
      return true;
    }

    curInode[0] = getChildINode(
            components[count[0] + 1],
            curInode[0].getId(),
            transactional);
    if (curInode[0] != null) {
      resolvedInodes.add(curInode[0]);
    }
    count[0] = count[0] + 1;

    return lastComp;
  }

  public static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }

  private static INode getChildINode(
          byte[] name,
          long parentId,
          boolean transactional)
          throws PersistanceException {
    String nameString = DFSUtil.bytes2String(name);
    if (transactional) {
      // TODO - Memcache success check - do primary key instead.
      LOG.debug("about to acquire lock on " + DFSUtil.bytes2String(name));
      return EntityManager.find(INode.Finder.ByNameAndParentId, nameString, parentId);
    } else {
      return findINodeWithNoTransaction(nameString, parentId);
    }
  }

  private static INode findINodeWithNoTransaction(
          String name,
          long parentId)
          throws StorageException {
    LOG.info(String.format(
            "Read inode with no transaction by parent-id=%d, name=%s",
            parentId,
            name));
    InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
    return da.findInodeByNameAndParentId(name, parentId);
  }

  public static LinkedList<INode> resolvePathWithNoTransaction(
          String path,
          boolean resolveLink)
          throws UnresolvedPathException, PersistanceException {
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
      resolvedInodes.add(readRoot());
      return resolvedInodes;
    } else {
      curNode[0] = readRoot();
    }

    while (count[0] < components.length && curNode[0] != null) {

      lastComp = INodeUtil.getNextChild(
              curNode,
              components,
              count,
              resolvedInodes,
              resolveLink,
              false);
      if (lastComp) {
        break;
      }
    }

    return resolvedInodes;
  }

  public static long findINodeIdByBlock(long blockId) throws StorageException {
    LOG.info(String.format(
            "Read block with no transaction by bid=%d",
            blockId));
    BlockInfoDataAccess bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
    BlockInfo bInfo = bda.findById(blockId);
    if (bInfo == null) {
      return INode.NON_EXISTING_ID;
    }
    return bInfo.getInodeId();
  }

  public static LinkedList<INode> findPathINodesById(long inodeId) throws PersistanceException {
    LinkedList<INode> pathInodes = new LinkedList<INode>();
    if (inodeId != INode.NON_EXISTING_ID) {
      INode inode = readById(inodeId);
      if (inode == null) {
        return pathInodes;
      }
      readFromLeafToRoot(inode, pathInodes);
    }
    return pathInodes;
  }

  public static SortedSet<String> findPathsByLeaseHolder(String holder) throws StorageException {
    SortedSet<String> sortedPaths = new TreeSet<String>();
    LeaseDataAccess lda = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
    Lease rcLease = lda.findByPKey(holder);
    if (rcLease == null) {
      return sortedPaths;
    }
    LeasePathDataAccess pda = (LeasePathDataAccess) StorageFactory.getDataAccess(LeasePathDataAccess.class);
    Collection<LeasePath> rclPaths = pda.findByHolderId(rcLease.getHolderID());
    for (LeasePath lp : rclPaths) {
      sortedPaths.add(lp.getPath()); // sorts paths in order to lock paths in the lexicographic order.
    }
    return sortedPaths;
  }

  private static INode readRoot() throws StorageException {
    return readById(FSDirectory.ROOT_ID);
  }

  private static INode readById(long id) throws StorageException {
    LOG.info(String.format(
            "Read inode with no transaction by id=%d",
            id));
    InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
    return da.findInodeById(id);
  }

  private static void readFromLeafToRoot(INode inode, LinkedList<INode> list) throws PersistanceException {
    if (inode.getParentId() == -1) {
      list.add(inode);
      return;
    }

    readFromLeafToRoot(readById(inode.getParentId()), list);
    INode i = readById(inode.getId());
    list.add(i);
  }
}
