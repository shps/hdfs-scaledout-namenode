package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.INodeStorage;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class INodeDerby extends INodeStorage {

  private DerbyConnector connector = DerbyConnector.INSTANCE;
  protected Map<Long, INode> newInodes = new HashMap<Long, INode>();

  @Override
  protected INode findInodeById(long inodeId) {
    Connection conn = connector.obtainSession();
    String query = String.format("select * from %s where %s=?", TABLE_NAME, ID);
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, inodeId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return createInode(rSet);
      }
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SQLException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  @Override
  protected List<INode> findInodesByParentIdSortedByName(long parentId) {
    Connection conn = connector.obtainSession();
    String query = String.format("select * from %s where %s=?", TABLE_NAME, PARENT_ID);
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, parentId);
      ResultSet rSet = s.executeQuery();
      return createInodeList(rSet);
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
      return null;
    } catch (SQLException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
      return null;
    }
  }

  @Override
  protected INode findInodeByNameAndParentId(String name, long parentId) {
    Connection conn = connector.obtainSession();
    String query = String.format("select * from %s where %s=? and %s=?",
            TABLE_NAME, NAME, PARENT_ID);
    try {
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, name);
      s.setLong(2, parentId);
      ResultSet rSet = s.executeQuery();
      if (rSet.next()) {
        return createInode(rSet);
      }
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
    } catch (SQLException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
    }
    
    return null;
  }

  @Override
  protected List<INode> findInodesByIds(List<Long> ids) {
    Connection conn = connector.obtainSession();
    if (ids.size() > 0) {
      StringBuilder query = new StringBuilder("select * from ").append(TABLE_NAME).
              append(" where ").append(ID).append("=").append(ids.get(0));
      for (int i = 1; i < ids.size(); i++) {
        query.append(" or ").append(ID).append("=").append(ids.get(i));
      }
      try {
        ResultSet rSet = conn.prepareStatement(query.toString()).executeQuery();
        return createInodeList(rSet);
      } catch (IOException ex) {
        Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
        return null;
      } catch (SQLException ex) {
        Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
        return null;
      }
    }
    return null;
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commit() {

    String delete = String.format("delete from %s where %s=?", TABLE_NAME, ID);
    String insert = String.format("insert into %s(%s,%s,%s,%s,"
            + "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
            + "%s, %s, %s, %s) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", TABLE_NAME, IS_DIR_WITH_QUOTA,
            IS_DIR, IS_UNDER_CONSTRUCTION, MODIFICATION_TIME, ACCESS_TIME, NAME, PERMISSION,
            PARENT_ID, NSQUOTA, DSQUOTA, NSCOUNT, DSCOUNT, HEADER, CLIENT_NAME, CLIENT_MACHINE,
            CLIENT_NODE, SYMLINK, ID);
    String update = String.format("update %s set %s=?, %s=?, %s=?,"
            + "%s=?, %s=?, %s=?, %s=?, %s=?, %s=?, %s=?, %s=?, %s=?, %s=?,"
            + "%s=?, %s=?, %s=?, %s=? where %s=?", TABLE_NAME, IS_DIR_WITH_QUOTA,
            IS_DIR, IS_UNDER_CONSTRUCTION, MODIFICATION_TIME, ACCESS_TIME, NAME, PERMISSION,
            PARENT_ID, NSQUOTA, DSQUOTA, NSCOUNT, DSCOUNT, HEADER, CLIENT_NAME, CLIENT_MACHINE,
            CLIENT_NODE, SYMLINK, ID);
    Connection conn = connector.obtainSession();

    try {
      PreparedStatement insrt = conn.prepareStatement(insert);
      for (INode inode : newInodes.values()) {
        createUpdateQuery(inode, insrt);
        insrt.addBatch();
      }
      insrt.executeBatch();
      insrt.close();

      PreparedStatement updt = conn.prepareStatement(update);
      for (INode inode : modifiedInodes.values()) {
        createUpdateQuery(inode, updt);
        updt.addBatch();
      }
      updt.executeBatch();
      updt.close();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (INode inode : removedInodes.values()) {
        dlt.setLong(1, inode.getId());
        dlt.addBatch();
      }
      dlt.executeBatch();
      dlt.close();
    } catch (SQLException ex) {
      SQLException next;
      while ((next = ex.getNextException()) != null)
      {
        Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, next);
      }
    }
  }

  @Override
  public void add(INode inode) throws TransactionContextException {
    if (removedInodes.containsKey(inode.getId())) {
      throw new TransactionContextException("Removed  inode passed to be persisted");
    }

    inodesIdIndex.put(inode.getId(), inode);
    inodesNameParentIndex.put(inode.nameParentKey(), inode);
    newInodes.put(inode.getId(), inode);
  }

  @Override
  public void clear() {
    super.clear();
    newInodes.clear();
  }

  @Override
  public void remove(INode inode) throws TransactionContextException {
    super.remove(inode);
    newInodes.remove(inode.getId());
  }

  private void createUpdateQuery(INode inode, PreparedStatement s) throws SQLException {

    if ((inode instanceof INodeDirectoryWithQuota)) {
      s.setBytes(1, new byte[]{(byte)1});
    } else {
      s.setBytes(1, new byte[]{(byte)0});
    }

    if (inode instanceof INodeDirectory) {
      s.setBytes(2, new byte[]{(byte)1});
    } else {
      s.setBytes(2, new byte[]{(byte)0});
    }

    if (inode instanceof INodeFile) {
      if (((INodeFile) inode).isUnderConstruction()) {
        s.setBytes(3, new byte[]{(byte)1});
      } else {
        s.setBytes(3, new byte[]{(byte)0});
      }
    } else {
      s.setBytes(3, new byte[]{(byte)0});
    }

    s.setLong(4, inode.getModificationTime());
    s.setLong(5, inode.getAccessTime());
    s.setString(6, inode.getName());

    DataOutputBuffer permissionString = new DataOutputBuffer();
    try {
      inode.getPermissionStatus().write(permissionString);
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    s.setBytes(7, permissionString.getData());
    s.setLong(8, inode.getParentId());
    s.setLong(9, inode.getNsQuota());
    s.setLong(10, inode.getDsQuota());

    if (inode instanceof INodeDirectory) {
      s.setLong(11, ((INodeDirectory) inode).getNsCount());
      s.setLong(12, ((INodeDirectory) inode).getDsCount());
    } else {
      s.setNull(11, Types.BIGINT);
      s.setNull(12, Types.BIGINT);
    }

    if (inode instanceof INodeFile) {
      s.setLong(13, getHeader(((INodeFile) inode).getReplication(), ((INodeFile) inode).getPreferredBlockSize()));
      s.setString(14, ((INodeFile) inode).getClientName());
      s.setString(15, ((INodeFile) inode).getClientMachine());
      if (((INodeFile) inode).getClientNode() != null) {
        s.setString(16, ((INodeFile) inode).getClientNode().getName());
      } else {
        s.setNull(16, Types.VARCHAR);
      }
    } else {
      s.setNull(13, Types.BIGINT);
      s.setNull(14, Types.VARCHAR);
      s.setNull(15, Types.VARCHAR);
      s.setNull(16, Types.VARCHAR);
    }

    if (inode instanceof INodeSymlink) {
      String linkValue = DFSUtil.bytes2String(((INodeSymlink) inode).getSymlink());
      s.setString(17, linkValue);
    } else {
      s.setNull(17, Types.VARCHAR);
    }

    s.setLong(18, inode.getId());
  }

  private INode createInode(ResultSet rSet) throws IOException, SQLException {
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(rSet.getBytes(PERMISSION), rSet.getBytes(PERMISSION).length);
    PermissionStatus ps = PermissionStatus.read(buffer);

    INode inode = null;

    if (rSet.getBytes(IS_DIR)[0] == 1) {
      if (rSet.getBytes(IS_DIR_WITH_QUOTA)[0] == 1) {
        inode = new INodeDirectoryWithQuota(rSet.getString(NAME), ps, rSet.getLong(NSQUOTA),
                rSet.getLong(DSQUOTA));
      } else {
        String iname = (rSet.getString(NAME).length() == 0) ? INodeDirectory.ROOT_NAME : rSet.getString(NAME);
        inode = new INodeDirectory(iname, ps);
      }

      inode.setAccessTime(rSet.getLong(ACCESS_TIME));
      inode.setModificationTime(rSet.getLong(MODIFICATION_TIME));
      ((INodeDirectory) (inode)).setSpaceConsumed(rSet.getLong(NSCOUNT), rSet.getLong(DSCOUNT));
    } else if (rSet.getString(SYMLINK) != null) {
      inode = new INodeSymlink(rSet.getString(SYMLINK), rSet.getLong(MODIFICATION_TIME),
              rSet.getLong(ACCESS_TIME), ps);
    } else {

      inode = new INodeFile(rSet.getBytes(IS_UNDER_CONSTRUCTION)[0] == 1 ? true : false,
              rSet.getString(NAME).getBytes(),
              getReplication(rSet.getLong(HEADER)),
              rSet.getLong(MODIFICATION_TIME),
              getPreferredBlockSize(rSet.getLong(HEADER)),
              ps,
              rSet.getString(CLIENT_NAME),
              rSet.getString(CLIENT_MACHINE),
              (rSet.getString(CLIENT_NODE) == null
              || rSet.getString(CLIENT_NODE).isEmpty()) ? null : new DatanodeID(rSet.getString(CLIENT_NODE)));
      inode.setAccessTime(rSet.getLong(ACCESS_TIME));
    }

    inode.setId(rSet.getLong(ID));
    inode.setName(rSet.getString(NAME));
    inode.setParentId(rSet.getLong(PARENT_ID));

    return inode;
  }

  private List<INode> createInodeList(ResultSet rSet) throws IOException, SQLException {
    List<INode> inodes = new ArrayList<INode>();
    while (rSet.next()) {
      inodes.add(createInode(rSet));
    }
    return inodes;
  }
}
