package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
      } else {
        return null;
      }
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
      return null;
    } catch (SQLException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
      return null;
    }
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
      } else {
        return null;
      }
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
      return null;
    } catch (SQLException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
      return null;
    }
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
    Connection conn = connector.obtainSession();
    try {
      for (INode inode : newInodes.values()) {
        conn.prepareStatement(createInsertQuery(inode)).executeUpdate();
      }

      for (INode inode : modifiedInodes.values()) {
        conn.prepareStatement(createUpdateQuery(inode)).executeUpdate();
      }

      PreparedStatement dlt = conn.prepareStatement(delete);
      for (INode inode : newInodes.values()) {
        dlt.setLong(1, inode.getId());
        dlt.addBatch();
      }
      dlt.executeBatch();
    } catch (SQLException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
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

  private String createInsertQuery(INode inode) throws SQLException {
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();

    columns.append(ID);
    values.append(inode.getId());

    columns.append(", ");
    columns.append(IS_DIR_WITH_QUOTA);
    if ((inode instanceof INodeDirectoryWithQuota)) {
      values.append(",1");
    } else {
      values.append(",0");
    }

    columns.append(", ");
    columns.append(IS_DIR);
    if (inode instanceof INodeDirectory) {
      values.append(",1");
    } else {
      values.append(",0");
    }

    columns.append(IS_UNDER_CONSTRUCTION + ", ");
    if (inode instanceof INodeFile) {
      values.append(",1");
    } else {
      values.append(",0");
    }
    columns.append(", ");
    columns.append(MODIFICATION_TIME);
    values.append(", ");
    values.append(inode.getModificationTime());
    columns.append(", ");
    columns.append(ACCESS_TIME);
    values.append(", ");
    values.append(inode.getAccessTime());
    columns.append(", ");
    columns.append(NAME);
    values.append(", ");
    values.append(inode.getName());

    DataOutputBuffer permissionString = new DataOutputBuffer();
    try {
      inode.getPermissionStatus().write(permissionString);
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    columns.append(", ");
    columns.append(PERMISSION);
    values.append(", ");
    values.append(permissionString.getData());
    columns.append(", ");
    columns.append(PARENT_ID);
    values.append(", ");
    values.append(inode.getParentId());
    columns.append(", ");
    columns.append(NSQUOTA);
    values.append(", ");
    values.append(inode.getNsQuota());
    columns.append(", ");
    columns.append(DSQUOTA);
    values.append(", ");
    values.append(inode.getDsQuota());

    if (inode instanceof INodeDirectory) {
      columns.append(", ");
      columns.append(NSCOUNT);
      values.append(", ");
      values.append(((INodeDirectory) inode).getNsCount());
      columns.append(", ");
      columns.append(DSCOUNT);
      values.append(", ");
      values.append(((INodeDirectory) inode).getDsCount());
    }

    if (inode instanceof INodeFile) {
      columns.append(", ");
      columns.append(HEADER);
      values.append(", ");
      values.append(getHeader(((INodeFile) inode).getReplication(), ((INodeFile) inode).getPreferredBlockSize()));
      columns.append(", ");
      columns.append(CLIENT_NAME);
      values.append(", ");
      values.append(((INodeFile) inode).getClientName());
      columns.append(", ");
      columns.append(CLIENT_MACHINE);
      values.append(", ");
      values.append(((INodeFile) inode).getClientMachine());
      columns.append(", ");
      columns.append(CLIENT_NODE);
      values.append(", ");
      values.append(((INodeFile) inode).getClientNode() == null ? null : ((INodeFile) inode).getClientNode().getName());
    }

    if (inode instanceof INodeSymlink) {
      columns.append(", ");
      columns.append(SYMLINK);
      values.append(", ");
      String linkValue = DFSUtil.bytes2String(((INodeSymlink) inode).getSymlink());
      values.append(linkValue);
    }

    return "insert into " + TABLE_NAME + "(" + columns + ") values(" + values + ")";
  }

  private String createUpdateQuery(INode inode) throws SQLException {
    StringBuilder query = new StringBuilder("update ").append(TABLE_NAME).append(" ");

    query.append("set ").append(IS_DIR_WITH_QUOTA).append("=");
    if ((inode instanceof INodeDirectoryWithQuota)) {
      query.append(",1");
    } else {
      query.append(",0");
    }

    query.append(", ").append("set ").append(IS_DIR).append("=");
    if (inode instanceof INodeDirectory) {
      query.append(",1");
    } else {
      query.append(",0");
    }

    query.append(", ").append("set ").append(IS_UNDER_CONSTRUCTION).append("=");
    if (inode instanceof INodeFile) {
      query.append(",1");
    } else {
      query.append(",0");
    }
    query.append(", ").append("set ").append(MODIFICATION_TIME).append("=").append(inode.getModificationTime());
    query.append(", ").append("set ").append(ACCESS_TIME).append("=").append(inode.getAccessTime());
    query.append(", ").append("set ").append(NAME).append("=").append(inode.getName());

    DataOutputBuffer permissionString = new DataOutputBuffer();
    try {
      inode.getPermissionStatus().write(permissionString);
    } catch (IOException ex) {
      Logger.getLogger(INodeDerby.class.getName()).log(Level.SEVERE, null, ex);
    }

    query.append(", ").append("set ").append(PERMISSION).append("=").append(permissionString.getData());
    query.append(", ").append("set ").append(PARENT_ID).append("=").append(inode.getParentId());
    query.append(", ").append("set ").append(NSQUOTA).append("=").append(inode.getNsQuota());
    query.append(", ").append("set ").append(DSQUOTA).append("=").append(inode.getDsQuota());

    if (inode instanceof INodeDirectory) {
      query.append(", ").append("set ").append(NSCOUNT).append("=").append(((INodeDirectory) inode).getNsCount());
      query.append(", ").append("set ").append(DSCOUNT).append("=").append(((INodeDirectory) inode).getDsCount());
    }

    if (inode instanceof INodeFile) {
      query.append(", ").append("set ").append(HEADER).append("=").
              append(getHeader(((INodeFile) inode).getReplication(), ((INodeFile) inode).getPreferredBlockSize()));
      query.append(", ").append("set ").append(CLIENT_NAME).append("=").append(((INodeFile) inode).getClientName());
      query.append(", ").append("set ").append(CLIENT_MACHINE).append("=").append(((INodeFile) inode).getClientMachine());
      query.append(", ").append("set ").append(CLIENT_NODE).append("=").
              append(((INodeFile) inode).getClientNode() == null ? null : ((INodeFile) inode).getClientNode().getName());
    }

    if (inode instanceof INodeSymlink) {
      String linkValue = DFSUtil.bytes2String(((INodeSymlink) inode).getSymlink());
      query.append(", ").append("set ").append(SYMLINK).append("=").append(linkValue);
    }

    query.append(" where ").append(ID).append("=").append(inode.getId());
    return query.toString();
  }

  private INode createInode(ResultSet rSet) throws IOException, SQLException {
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(rSet.getBytes(PERMISSION), rSet.getBytes(PERMISSION).length);
    PermissionStatus ps = PermissionStatus.read(buffer);

    INode inode = null;

    if (rSet.getBoolean(IS_DIR)) {
      if (rSet.getBoolean(IS_DIR_WITH_QUOTA)) {
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

      inode = new INodeFile(rSet.getBoolean(IS_UNDER_CONSTRUCTION), rSet.getString(NAME).getBytes(),
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
