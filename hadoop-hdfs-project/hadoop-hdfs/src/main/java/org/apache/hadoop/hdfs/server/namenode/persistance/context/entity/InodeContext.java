package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.*;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InodeDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InodeContext extends EntityContext<INode> {

  /**
   * Mappings
   */
  protected Map<Long, INode> inodesIdIndex = new HashMap<Long, INode>();
  protected Map<String, INode> inodesNameParentIndex = new HashMap<String, INode>();
  protected Map<Long, List<INode>> inodesParentIndex = new HashMap<Long, List<INode>>();
  protected Map<Long, INode> newInodes = new HashMap<Long, INode>();
  protected Map<Long, INode> modifiedInodes = new HashMap<Long, INode>();
  protected Map<Long, INode> removedInodes = new HashMap<Long, INode>();
  InodeDataAccess dataAccess;

  public InodeContext(InodeDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(INode inode) throws PersistanceException {
    if (removedInodes.containsKey(inode.getId())) {
      throw new TransactionContextException("Removed  inode passed to be persisted");
    }

    inodesIdIndex.put(inode.getId(), inode);
    inodesNameParentIndex.put(inode.nameParentKey(), inode);
    newInodes.put(inode.getId(), inode);
    log("added-inode", CacheHitState.NA,
            new String[]{"id", Long.toString(inode.getId()), "name", inode.getName(),
            "pid", Long.toString(inode.getParentId())});
  }

  @Override
  public int count(CounterType<INode> counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clear() {
    inodesIdIndex.clear();
    inodesNameParentIndex.clear();
    inodesParentIndex.clear();
    removedInodes.clear();
    newInodes.clear();
    modifiedInodes.clear();
  }

  @Override
  public INode find(FinderType<INode> finder, Object... params) throws PersistanceException {
    INode.Finder iFinder = (INode.Finder) finder;
    INode result = null;
    switch (iFinder) {
      case ByPKey:
        long inodeId = (Long) params[0];
        if (removedInodes.containsKey(inodeId)) {
          log("find-inode-by-pk-removed", CacheHitState.HIT, new String[]{"id", Long.toString(inodeId)});
          result = null;
        } else if (inodesIdIndex.containsKey(inodeId)) {
          log("find-inode-by-pk", CacheHitState.HIT, new String[]{"id", Long.toString(inodeId)});
          result = inodesIdIndex.get(inodeId);
        } else {
          log("find-inode-by-pk", CacheHitState.LOSS, new String[]{"id", Long.toString(inodeId)});
          result = dataAccess.findInodeById(inodeId);
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
          log("find-inode-by-name-parentid", CacheHitState.HIT,
                  new String[]{"name", name, "pid", Long.toString(parentId)});
          result = inodesNameParentIndex.get(key);
        } else {
          result = dataAccess.findInodeByNameAndParentId(name, parentId);
          if (result != null) {
            if (removedInodes.containsKey(result.getId())) {
              log("find-inode-by-name-parentid-removed", CacheHitState.LOSS,
                      new String[]{"name", name, "pid", Long.toString(parentId)});
              return null;
            }
            inodesIdIndex.put(result.getId(), result);
            inodesNameParentIndex.put(result.nameParentKey(), result);
          }
          log("find-inode-by-name-parentid", CacheHitState.LOSS, new String[]{"name", name, "pid", Long.toString(parentId)});
        }
        break;
    }

    return result;
  }

  @Override
  public Collection<INode> findList(FinderType<INode> finder, Object... params) throws PersistanceException {
    INode.Finder iFinder = (INode.Finder) finder;
    List<INode> result = null;
    switch (iFinder) {
      case ByParentId:
        long parentId = (Long) params[0];
        if (inodesParentIndex.containsKey(parentId)) {
          log("find-inodes-by-parentid", CacheHitState.HIT, new String[]{"pid", Long.toString(parentId)});
          result = inodesParentIndex.get(parentId);
        } else {
          log("find-inodes-by-parentid", CacheHitState.LOSS, new String[]{"pid", Long.toString(parentId)});
          result = syncInodeInstances(dataAccess.findInodesByParentIdSortedByName(parentId));
          Collections.sort(result, INode.Order.ByName);
          inodesParentIndex.put(parentId, result);
        }
        break;
      case ByIds:
        List<Long> ids = (List<Long>) params[0];
        log("find-inodes-by-ids", CacheHitState.NA, new String[]{"ids", ids.toString()});
        result = syncInodeInstances(dataAccess.findInodesByIds(ids));
        break;
    }

    return result;
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedInodes.values(), newInodes.values(), modifiedInodes.values());
  }

  @Override
  public void remove(INode inode) throws PersistanceException {
    inodesIdIndex.remove(inode.getId());
    inodesNameParentIndex.remove(inode.nameParentKey());
    newInodes.remove(inode.getId());
    modifiedInodes.remove(inode.getId());
    removedInodes.put(inode.getId(), inode);
    log("removed-inode", CacheHitState.NA, new String[]{"id", Long.toString(inode.getId()), "name", inode.getName()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(INode inode) throws PersistanceException {

    if (removedInodes.containsKey(inode.getId())) {
      throw new TransactionContextException("Removed  inode passed to be persisted");
    }

    inodesIdIndex.put(inode.getId(), inode);
    inodesNameParentIndex.put(inode.nameParentKey(), inode);
    modifiedInodes.put(inode.getId(), inode);
    log("updated-inode", CacheHitState.NA, new String[]{"id", Long.toString(inode.getId()), "name", inode.getName()});
  }

  private List<INode> syncInodeInstances(List<INode> newInodes) {
    List<INode> finalList = new ArrayList<INode>();

    for (INode inode : newInodes) {
      if (removedInodes.containsKey(inode.getId())) {
        continue;
      }
      if (inodesIdIndex.containsKey(inode.getId())) {
        finalList.add(inodesIdIndex.get(inode.getId()));
      } else {
        inodesIdIndex.put(inode.getId(), inode);
        inodesNameParentIndex.put(inode.nameParentKey(), inode);
        finalList.add(inode);
      }
    }

    return finalList;
  }
}
