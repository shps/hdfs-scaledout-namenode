package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.INodeContext;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

@PersistenceCapable(table = INodeContext.TABLE_NAME)
interface INodeTable {

  @PrimaryKey
  @Column(name = INodeContext.ID)
  long getId();     // id of the inode

  void setId(long id);

  @Column(name = INodeContext.NAME)
  @Index(name = "path_lookup_idx")
  String getName();     //name of the inode

  void setName(String name);

  //id of the parent inode 
  @Column(name = INodeContext.PARENT_ID)
  @Index(name = "path_lookup_idx")
  long getParentId();     // id of the inode

  void setParentId(long parentid);

  // marker for InodeDirectory
  @Column(name = INodeContext.IS_DIR)
  boolean getIsDir();

  void setIsDir(boolean isDir);

  // marker for InodeDirectoryWithQuota
  @Column(name = INodeContext.IS_DIR_WITH_QUOTA)
  boolean getIsDirWithQuota();

  void setIsDirWithQuota(boolean isDirWithQuota);

  // Inode
  @Column(name = INodeContext.MODIFICATION_TIME)
  long getModificationTime();

  void setModificationTime(long modificationTime);

  // Inode
  @Column(name = INodeContext.ACCESS_TIME)
  long getATime();

  void setATime(long modificationTime);

  // Inode
  @Column(name = INodeContext.PERMISSION)
  byte[] getPermission();

  void setPermission(byte[] permission);

  // InodeDirectoryWithQuota
  @Column(name = INodeContext.NSCOUNT)
  long getNSCount();

  void setNSCount(long nsCount);

  // InodeDirectoryWithQuota
  @Column(name = INodeContext.DSCOUNT)
  long getDSCount();

  void setDSCount(long dsCount);

  // InodeDirectoryWithQuota
  @Column(name = INodeContext.NSQUOTA)
  long getNSQuota();

  void setNSQuota(long nsQuota);

  // InodeDirectoryWithQuota
  @Column(name = INodeContext.DSQUOTA)
  long getDSQuota();

  void setDSQuota(long dsQuota);

  //  marker for InodeFileUnderConstruction
  @Column(name = INodeContext.IS_UNDER_CONSTRUCTION)
  boolean getIsUnderConstruction();

  void setIsUnderConstruction(boolean isUnderConstruction);

  // InodeFileUnderConstruction
  @Column(name = INodeContext.CLIENT_NAME)
  String getClientName();

  void setClientName(String isUnderConstruction);

  // InodeFileUnderConstruction
  @Column(name = INodeContext.CLIENT_MACHINE)
  String getClientMachine();

  void setClientMachine(String clientMachine);

  @Column(name = INodeContext.CLIENT_NODE)
  String getClientNode();

  void setClientNode(String clientNode);

  //  marker for InodeFile
  @Column(name = INodeContext.IS_CLOSED_FILE)
  boolean getIsClosedFile();

  void setIsClosedFile(boolean isClosedFile);

  // InodeFile
  @Column(name = INodeContext.HEADER)
  long getHeader();

  void setHeader(long header);

  //INodeSymlink
  @Column(name = INodeContext.SYMLINK)
  String getSymlink();

  void setSymlink(String symlink);
}

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class INodeClusterj extends INodeContext {

  private Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  public void prepare() {
    for (INode inode : modifiedInodes.values()) {
      INodeTable persistable = session.newInstance(INodeTable.class);
      createPersistable(inode, persistable);
      session.savePersistent(persistable);
    }

    for (INode inode : removedInodes.values()) {
      INodeTable persistable = session.newInstance(INodeTable.class, inode.getId());
      session.deletePersistent(persistable);
    }
  }

  @Override
  protected INode findInodeById(long inodeId) {
    try {
      INodeTable persistable = session.find(INodeTable.class, inodeId);

      if (persistable
              == null) {
        return null;
      }
      INode inode = createInode(persistable);

      return inode;
    } catch (IOException ex) {
      Logger.getLogger(INodeClusterj.class.getName()).log(Level.SEVERE, null, ex);
      return null;
    }

  }

  private INode createInode(INodeTable persistable) throws IOException {
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
              (persistable.getClientNode() == null || 
              persistable.getClientNode().isEmpty()) ? null : new DatanodeID(persistable.getClientNode()));
      inode.setAccessTime(persistable.getATime());
    }

    inode.setId(persistable.getId());
    inode.setName(persistable.getName());
    inode.setParentId(persistable.getParentId());

    return inode;
  }

  @Override
  protected List<INode> findInodesByParentIdSortedByName(long parentId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<INodeTable> dobj = qb.createQueryDefinition(INodeTable.class);
    Predicate pred1 = dobj.get("parentId").equal(dobj.param("parentID"));

    dobj.where(pred1);
    Query<INodeTable> query = session.createQuery(dobj);

    query.setParameter(
            "parentID", parentId);
    List<INodeTable> results = query.getResultList();
    List<INode> inodes = null;
    try {
      inodes = createInodeList(results);
    } catch (IOException ex) {
      Logger.getLogger(INodeClusterj.class.getName()).log(Level.SEVERE, null, ex);
    }

    List<INode> syncInodes = syncInodeInstances(inodes);
    Collections.sort(syncInodes, INode.Order.ByName);

    return syncInodes;
  }

  @Override
  protected INode findInodeByNameAndParentId(String name, long parentId) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<INodeTable> dobj = qb.createQueryDefinition(INodeTable.class);
    Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
    Predicate pred2 = dobj.get("parentId").equal(dobj.param("parentID"));

    dobj.where(pred1.and(pred2));
    Query<INodeTable> query = session.createQuery(dobj);

    query.setParameter(
            "name", name);
    query.setParameter(
            "parentID", parentId);
    List<INodeTable> results = query.getResultList();


    if (results.size()
            > 1) {
      try {
        throw new TransactionContextException("This parent has two chidlren with the same name");
      } catch (TransactionContextException ex) {
        Logger.getLogger(INodeClusterj.class.getName()).log(Level.SEVERE, null, ex);
        return null;
      }
    } else if (results.isEmpty()) {
      return null;
    } else {
      INode inode = null;
      try {
        inode = createInode(results.get(0));
      } catch (IOException ex) {
        Logger.getLogger(INodeClusterj.class.getName()).log(Level.SEVERE, null, ex);
      }

      return inode;
    }
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

  private List<INode> createInodeList(List<INodeTable> list) throws IOException {
    List<INode> inodes = new ArrayList<INode>();
    for (INodeTable persistable : list) {
      inodes.add(createInode(persistable));
    }
    return inodes;
  }

  @Override
  protected List<INode> findInodesByIds(List<Long> ids) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<INodeTable> dobj = qb.createQueryDefinition(INodeTable.class);
    PredicateOperand field = dobj.get("id");
    PredicateOperand values = dobj.param("param");
    Predicate predicate = field.in(values);

    dobj.where(predicate);
    Query<INodeTable> query = session.createQuery(dobj);

    query.setParameter(
            "param", ids.toArray());
    List<INodeTable> results = query.getResultList();
    List<INode> inodes = null;
    try {
      inodes = createInodeList(results);
    } catch (IOException ex) {
      Logger.getLogger(INodeClusterj.class.getName()).log(Level.SEVERE, null, ex);
    }
    List<INode> syncInodes = syncInodeInstances(inodes);

    return syncInodes;
  }

  private void createPersistable(INode inode, INodeTable persistable) {
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setName(inode.getName());

    DataOutputBuffer permissionString = new DataOutputBuffer();
    try {
      inode.getPermissionStatus().write(permissionString);
    } catch (IOException ex) {
      Logger.getLogger(INodeClusterj.class.getName()).log(Level.SEVERE, null, ex);
    }

    persistable.setPermission(permissionString.getData());
    persistable.setParentId(inode.getParentId());
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

  @Override
  public int count(CounterType<INode> counter, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll() throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
