package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class INodeStorage implements Storage<INode> {

  public static final String TABLE_NAME = "inodes";
  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String PARENT_ID = "parent_id";
  public static final String IS_DIR = "is_dir";
  public static final String MODIFICATION_TIME = "modification_time";
  public static final String ACCESS_TIME = "access_time";
  public static final String PERMISSION = "permission";
  public static final String NSQUOTA = "nsquota";
  public static final String DSQUOTA = "dsquota";
  public static final String IS_UNDER_CONSTRUCTION = "is_under_construction";
  public static final String CLIENT_NAME = "client_name";
  public static final String CLIENT_MACHINE = "client_machine";
  public static final String CLIENT_NODE = "client_node";
  public static final String IS_CLOSED_FILE = "is_closed_file";
  public static final String HEADER = "header";
  public static final String IS_DIR_WITH_QUOTA = "is_dir_with_quota";
  public static final String NSCOUNT = "nscount";
  public static final String DSCOUNT = "dscount";
  public static final String SYMLINK = "symlink";
  /**
   * Mappings
   */
  protected Map<Long, INode> inodesIdIndex = new HashMap<Long, INode>();
  protected Map<String, INode> inodesNameParentIndex = new HashMap<String, INode>();
  protected Map<Long, List<INode>> inodesParentIndex = new HashMap<Long, List<INode>>();
  protected Map<Long, INode> modifiedInodes = new HashMap<Long, INode>();
  protected Map<Long, INode> removedInodes = new HashMap<Long, INode>();
  /**
   * 
   */
  /**
   * Number of bits for Block size
   */
  static final short BLOCKBITS = 48;
  /**
   * Header mask 64-bit representation Format: [16 bits for replication][48 bits
   * for PreferredBlockSize]
   */
  static final long HEADERMASK = 0xffffL << BLOCKBITS;

  @Override
  public void remove(INode inode) throws TransactionContextException {
    inodesIdIndex.remove(inode.getId());
    inodesNameParentIndex.remove(inode.nameParentKey());
    modifiedInodes.remove(inode.getId());

    removedInodes.put(inode.getId(), inode);
  }

  @Override
  public Collection<INode> findList(Finder<INode> finder, Object... params) {
    INodeFinder iFinder = (INodeFinder) finder;
    List<INode> result = null;
    switch (iFinder) {
      case ByParentId:
        long parentId = (Long) params[0];
        if (inodesParentIndex.containsKey(parentId)) {
          result = inodesParentIndex.get(parentId);
        } else {
          result = findInodesByParentIdSortedByName(parentId);
          inodesParentIndex.put(parentId, result);
        }
        break;
      case ByIds:
        List<Long> ids = (List<Long>) params[0];
        result = findInodesByIds(ids);
        break;
    }

    return result;
  }

  @Override
  public INode find(Finder<INode> finder, Object... params) {
    INodeFinder iFinder = (INodeFinder) finder;
    INode result = null;
    switch (iFinder) {
      case ByPKey:
        long inodeId = (Long) params[0];
        if (removedInodes.containsKey(inodeId)) {
          result = null;
        } else if (inodesIdIndex.containsKey(inodeId)) {
          result = inodesIdIndex.get(inodeId);
        } else {
          result = findInodeById(inodeId);
          if (result != null) {
            inodesIdIndex.put(inodeId, result);
            inodesNameParentIndex.put(result.nameParentKey(), result);
          }
        }
        break;
      case ByNameAndParentId:
        String name = (String) params[0];
        long parentId = (Long) params[1];
        String key = parentId + name;
        if (inodesNameParentIndex.containsKey(key)) {
          result = inodesNameParentIndex.get(key);
        } else {
          result = findInodeByNameAndParentId(name, parentId);
          if (result != null) {
            if (removedInodes.containsKey(result.getId())) {
              return null;
            }
            inodesIdIndex.put(result.getId(), result);
            inodesNameParentIndex.put(result.nameParentKey(), result);
          }
        }
        break;
    }

    return result;
  }

  @Override
  public void update(INode inode) throws TransactionContextException {

    if (removedInodes.containsKey(inode.getId())) {
      throw new TransactionContextException("Removed  inode passed to be persisted");
    }

    inodesIdIndex.put(inode.getId(), inode);
    inodesNameParentIndex.put(inode.nameParentKey(), inode);
    modifiedInodes.put(inode.getId(), inode);
  }

  @Override
  public void add(INode entity) throws TransactionContextException {
    update(entity);
  }

  @Override
  public void clear() {
    inodesIdIndex.clear();
    inodesNameParentIndex.clear();
    inodesParentIndex.clear();
    removedInodes.clear();
    modifiedInodes.clear();
  }

  protected abstract INode findInodeById(long inodeId);

  protected abstract List<INode> findInodesByParentIdSortedByName(long parentId);

  protected abstract INode findInodeByNameAndParentId(String name, long parentId);

  protected abstract List<INode> findInodesByIds(List<Long> ids);
  
  protected short getReplication(long header) {
    return (short) ((header & HEADERMASK) >> BLOCKBITS);
  }

  protected long getHeader(short replication, long preferredBlockSize) {
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

  protected long getPreferredBlockSize(long header) {
    return header & ~HEADERMASK;
  }
}
