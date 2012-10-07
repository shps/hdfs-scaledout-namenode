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
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InodeDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InodeClusterj extends InodeDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface InodeDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();     // id of the inode

    void setId(long id);

    @Column(name = NAME)
    @Index(name = "path_lookup_idx")
    String getName();     //name of the inode

    void setName(String name);

    //id of the parent inode 
    @Column(name = PARENT_ID)
    @Index(name = "path_lookup_idx")
    long getParentId();     // id of the inode

    void setParentId(long parentid);

    // marker for InodeDirectory
    @Column(name = IS_DIR)
    boolean getIsDir();

    void setIsDir(boolean isDir);

    // marker for InodeDirectoryWithQuota
    @Column(name = IS_DIR_WITH_QUOTA)
    boolean getIsDirWithQuota();

    void setIsDirWithQuota(boolean isDirWithQuota);

    // Inode
    @Column(name = MODIFICATION_TIME)
    long getModificationTime();

    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = ACCESS_TIME)
    long getATime();

    void setATime(long modificationTime);

    // Inode
    @Column(name = PERMISSION)
    byte[] getPermission();

    void setPermission(byte[] permission);

    // InodeDirectoryWithQuota
    @Column(name = NSCOUNT)
    long getNSCount();

    void setNSCount(long nsCount);

    // InodeDirectoryWithQuota
    @Column(name = DSCOUNT)
    long getDSCount();

    void setDSCount(long dsCount);

    // InodeDirectoryWithQuota
    @Column(name = NSQUOTA)
    long getNSQuota();

    void setNSQuota(long nsQuota);

    // InodeDirectoryWithQuota
    @Column(name = DSQUOTA)
    long getDSQuota();

    void setDSQuota(long dsQuota);

    //  marker for InodeFileUnderConstruction
    @Column(name = IS_UNDER_CONSTRUCTION)
    boolean getIsUnderConstruction();

    void setIsUnderConstruction(boolean isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_NAME)
    String getClientName();

    void setClientName(String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_MACHINE)
    String getClientMachine();

    void setClientMachine(String clientMachine);

    @Column(name = CLIENT_NODE)
    String getClientNode();

    void setClientNode(String clientNode);

    //  marker for InodeFile
    @Column(name = IS_CLOSED_FILE)
    boolean getIsClosedFile();

    void setIsClosedFile(boolean isClosedFile);

    // InodeFile
    @Column(name = HEADER)
    long getHeader();

    void setHeader(long header);

    //INodeSymlink
    @Column(name = SYMLINK)
    String getSymlink();

    void setSymlink(String symlink);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public void prepare(Collection<INode> removed, Collection<INode> newed, Collection<INode> modified) throws StorageException {
    Session session = connector.obtainSession();
    try {
      for (INode inode : newed) {
        InodeDTO persistable = session.newInstance(InodeDTO.class);
        createPersistable(inode, persistable);
        session.savePersistent(persistable);
      }

      for (INode inode : modified) {
        InodeDTO persistable = session.newInstance(InodeDTO.class);
        createPersistable(inode, persistable);
        session.savePersistent(persistable);
      }

      for (INode inode : removed) {
        InodeDTO persistable = session.newInstance(InodeDTO.class, inode.getId());
        session.deletePersistent(persistable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public INode findInodeById(long inodeId) throws StorageException {
    Session session = connector.obtainSession();
    try {
      InodeDTO persistable = session.find(InodeDTO.class, inodeId);

      if (persistable
              == null) {
        return null;
      }
      INode inode = createInode(persistable);

      return inode;
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<INode> findInodesByParentIdSortedByName(long parentId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();

      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      Predicate pred1 = dobj.get("parentId").equal(dobj.param("parentID"));
      dobj.where(pred1);
      Query<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("parentID", parentId);

      List<InodeDTO> results = query.getResultList();
      return createInodeList(results);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public INode findInodeByNameAndParentId(String name, long parentId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      
      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      
      Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
      Predicate pred2 = dobj.get("parentId").equal(dobj.param("parentID"));
      
      dobj.where(pred1.and(pred2));
      Query<InodeDTO> query = session.createQuery(dobj);
      
      query.setParameter(
              "name", name);
      query.setParameter(
              "parentID", parentId);
      List<InodeDTO> results = query.getResultList();

      if (results.size() > 1) {
        throw new StorageException("This parent has two chidlren with the same name");
      } else if (results.isEmpty()) {
        return null;
      } else {
        return createInode(results.get(0));
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }

  }

  @Override
  public List<INode> findInodesByIds(List<Long> ids) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
      PredicateOperand field = dobj.get("id");
      PredicateOperand values = dobj.param("param");
      Predicate predicate = field.in(values);

      dobj.where(predicate);
      Query<InodeDTO> query = session.createQuery(dobj);

      query.setParameter(
              "param", ids.toArray());
      List<InodeDTO> results = query.getResultList();
      List<INode> inodes = null;
      return createInodeList(results);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<INode> createInodeList(List<InodeDTO> list) throws IOException {
    List<INode> inodes = new ArrayList<INode>();
    for (InodeDTO persistable : list) {
      inodes.add(createInode(persistable));
    }
    return inodes;
  }

  private INode createInode(InodeDTO persistable) throws IOException {
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
              (persistable.getClientNode() == null
              || persistable.getClientNode().isEmpty()) ? null : new DatanodeID(persistable.getClientNode()));
      inode.setAccessTime(persistable.getATime());
    }

    inode.setId(persistable.getId());
    inode.setName(persistable.getName());
    inode.setParentId(persistable.getParentId());

    return inode;
  }

  private void createPersistable(INode inode, InodeDTO persistable) {
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setName(inode.getName());

    DataOutputBuffer permissionString = new DataOutputBuffer();
    try {
      inode.getPermissionStatus().write(permissionString);
    } catch (IOException ex) {
      Logger.getLogger(InodeClusterj.class.getName()).log(Level.SEVERE, null, ex);
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
}
