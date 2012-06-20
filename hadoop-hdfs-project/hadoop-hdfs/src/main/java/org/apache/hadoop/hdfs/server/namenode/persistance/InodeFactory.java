package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import se.sics.clusterj.INodeTableSimple;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class InodeFactory {

  /**
   * Number of bits for Block size
   */
  static final short BLOCKBITS = 48;
  /**
   * Header mask 64-bit representation Format: [16 bits for replication][48 bits
   * for PreferredBlockSize]
   */
  static final long HEADERMASK = 0xffffL << BLOCKBITS;

  public static void createPersistable(INode inode, INodeTableSimple persistable) {
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setName(inode.getName());

    DataOutputBuffer permissionString = new DataOutputBuffer();

    try {
      inode.getPermissionStatus().write(permissionString);
    } catch (IOException e) {
      e.printStackTrace();
    }

    persistable.setPermission(permissionString.getData());
    persistable.setParentID(inode.getParentId());
    persistable.setId(inode.getId());
    persistable.setNSQuota(inode.getNsQuota());
    persistable.setDSQuota(inode.getDsQuota());

    if (inode instanceof INodeDirectory) {
      persistable.setIsUnderConstruction(false);
      persistable.setIsDirWithQuota(false);
      persistable.setIsDir(true);
      persistable.setNSCount(((INodeDirectory) inode).getNsCount());
      persistable.setDSCount(((INodeDirectory) inode).getDsCount());
    }
    if (inode instanceof INodeDirectoryWithQuota) {
      persistable.setIsDir(true); //why was it false earlier?	    	
      persistable.setIsUnderConstruction(false);
      persistable.setIsDirWithQuota(true);
    }
    if (inode instanceof INodeFile) {
      persistable.setIsDir(false);
      persistable.setIsUnderConstruction(inode.isUnderConstruction());
      persistable.setIsDirWithQuota(false);
      persistable.setHeader(getHeader(((INodeFile) inode).getReplication(), ((INodeFile) inode).getPreferredBlockSize()));
      persistable.setClientName(((INodeFile) inode).getClientName());
      persistable.setClientMachine(((INodeFile) inode).getClientMachine());
      persistable.setClientNode(((INodeFile) inode).getClientNode() == null ? null : ((INodeFile) inode).getClientNode().getName());
    }
    if (inode instanceof INodeSymlink) {
      String linkValue = DFSUtil.bytes2String(((INodeSymlink) inode).getSymlink());
      persistable.setSymlink(linkValue);
    }
  }

  public static INode createInode(INodeTableSimple persistable) throws IOException {
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(persistable.getPermission(), persistable.getPermission().length);
    PermissionStatus ps = PermissionStatus.read(buffer);

    INode inode = null;

    if (persistable.getIsDir()) {
      if (persistable.getIsDirWithQuota()) {
        inode = new INodeDirectoryWithQuota(persistable.getName(), ps, persistable.getNSQuota(), persistable.getDSQuota());
      } else {
        String iname = (persistable.getName().length() == 0) ? INodeDirectory.ROOT_NAME : persistable.getName();
        inode = new INodeDirectory(iname, ps);
      }

      inode.setAccessTime(persistable.getATime());
      inode.setModificationTime(persistable.getModificationTime());
      ((INodeDirectory) (inode)).setSpaceConsumed(persistable.getNSCount(), persistable.getDSCount());
    } else if (persistable.getSymlink() != null) {
      inode = new INodeSymlink(persistable.getSymlink(), persistable.getModificationTime(),
              persistable.getATime(), ps);
    } else {

      inode = new INodeFile(persistable.getIsUnderConstruction(), persistable.getName().getBytes(),
              getReplication(persistable.getHeader()),
              persistable.getModificationTime(),
              getPreferredBlockSize(persistable.getHeader()),
              ps,
              persistable.getClientName(),
              persistable.getClientMachine(),
              (persistable.getClientNode() == null || persistable.getClientNode().isEmpty()) ? null: new DatanodeID(persistable.getClientNode()));
      inode.setAccessTime(persistable.getATime());
    }

    inode.setId(persistable.getId());
    inode.setName(persistable.getName());
    inode.setParentId(persistable.getParentID());

    return inode;
  }

  public static List<INode> createInodeList(List<INodeTableSimple> list) throws IOException {
    List<INode> inodes = new ArrayList<INode>();
    for (INodeTableSimple persistable : list) {
      inodes.add(createInode(persistable));
    }
    return inodes;
  }

  private static short getReplication(long header) {
    return (short) ((header & HEADERMASK) >> BLOCKBITS);
  }

  private static long getHeader(short replication, long preferredBlockSize) {
    long header = 0;

    if (replication <= 0) {
      throw new IllegalArgumentException("Unexpected value for the replication");
    }

    if ((preferredBlockSize < 0) || (preferredBlockSize > ~HEADERMASK)) {
      throw new IllegalArgumentException("Unexpected value for the block size");
    }

    header = (header & HEADERMASK) | (preferredBlockSize & ~HEADERMASK);
    header = ((long) replication << BLOCKBITS) | (header & ~HEADERMASK);

    return header;
  }

  private static long getPreferredBlockSize(long header) {
    return header & ~HEADERMASK;
  }
}
