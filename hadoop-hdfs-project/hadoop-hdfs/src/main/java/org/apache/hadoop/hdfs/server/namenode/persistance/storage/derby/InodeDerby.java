package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class InodeDerby extends InodeDataAccess {

  private DerbyConnector connector = DerbyConnector.INSTANCE;

  @Override
  public INode findInodeById(long inodeId) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s=?", TABLE_NAME, ID);
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, inodeId);
      ResultSet rSet = s.executeQuery();
      INode result = null;
      if (rSet.next()) {
        result = createInode(rSet);
      }
      return result;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  @Override
  public List<INode> findInodesByParentIdSortedByName(long parentId) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s=?", TABLE_NAME, PARENT_ID);
      List<INode> inodes = null;
      PreparedStatement s = conn.prepareStatement(query);
      s.setLong(1, parentId);
      ResultSet rSet = s.executeQuery();
      return createInodeList(rSet);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public INode findInodeByNameAndParentId(String name, long parentId) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      String query = String.format("select * from %s where %s=? and %s=?",
              TABLE_NAME, NAME, PARENT_ID);
      PreparedStatement s = conn.prepareStatement(query);
      s.setString(1, name);
      s.setLong(2, parentId);
      ResultSet rSet = s.executeQuery();
      INode result = null;
      if (rSet.next()) {
        result = createInode(rSet);
      }
      return result;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return null;
    }
  }

  @Override
  public List<INode> findInodesByIds(List<Long> ids) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      StringBuilder query = new StringBuilder("select * from ").append(TABLE_NAME).
              append(" where ").append(ID).append("=").append(ids.get(0));
      for (int i = 1; i < ids.size(); i++) {
        query.append(" or ").append(ID).append("=").append(ids.get(i));
      }
      ResultSet rSet = conn.prepareStatement(query.toString()).executeQuery();
      return createInodeList(rSet);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } catch (SQLException ex) {
      handleSQLException(ex);
      return Collections.EMPTY_LIST;
    }
  }

  @Override
  public void prepare(Collection<INode> removed, Collection<INode> newed, Collection<INode> modified) throws StorageException {

    try {
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

      PreparedStatement insrt = conn.prepareStatement(insert);
      for (INode inode : newed) {
        createUpdateQuery(inode, insrt);
        insrt.addBatch();
      }
      insrt.executeBatch();
      insrt.close();

      PreparedStatement updt = conn.prepareStatement(update);
      for (INode inode : modified) {
        createUpdateQuery(inode, updt);
        updt.addBatch();
      }
      updt.executeBatch();
      updt.close();

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (INode inode : removed) {
        dlt.setLong(1, inode.getId());
        dlt.addBatch();
      }
      dlt.executeBatch();
      dlt.close();
    } catch (SQLException ex) {
      handleSQLException(ex);
    }
  }

  private void createUpdateQuery(INode inode, PreparedStatement s) throws SQLException {

    if ((inode instanceof INodeDirectoryWithQuota)) {
      s.setBytes(1, new byte[]{(byte) 1});
    } else {
      s.setBytes(1, new byte[]{(byte) 0});
    }

    if (inode instanceof INodeDirectory) {
      s.setBytes(2, new byte[]{(byte) 1});
    } else {
      s.setBytes(2, new byte[]{(byte) 0});
    }

    if (inode instanceof INodeFile) {
      if (((INodeFile) inode).isUnderConstruction()) {
        s.setBytes(3, new byte[]{(byte) 1});
      } else {
        s.setBytes(3, new byte[]{(byte) 0});
      }
    } else {
      s.setBytes(3, new byte[]{(byte) 0});
    }

    s.setLong(4, inode.getModificationTime());
    s.setLong(5, inode.getAccessTime());
    s.setString(6, inode.getName());

    DataOutputBuffer permissionString = new DataOutputBuffer();
    try {
      inode.getPermissionStatus().write(permissionString);
    } catch (IOException ex) {
      Logger.getLogger(InodeDerby.class.getName()).log(Level.SEVERE, null, ex);
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
