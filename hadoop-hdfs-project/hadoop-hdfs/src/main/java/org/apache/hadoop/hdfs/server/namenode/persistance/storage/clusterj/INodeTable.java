/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Index;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.INodeStorage;

@PersistenceCapable(table = INodeStorage.TABLE_NAME)
public interface INodeTable {

  @PrimaryKey
  @Column(name = INodeStorage.ID)
  long getId();     // id of the inode

  void setId(long id);

  @Column(name = INodeStorage.NAME)
  @Index(name = "path_lookup_idx")
  String getName();     //name of the inode

  void setName(String name);

  //id of the parent inode 
  @Column(name = INodeStorage.PARENT_ID)
  @Index(name = "path_lookup_idx")
  long getParentId();     // id of the inode

  void setParentId(long parentid);

  // marker for InodeDirectory
  @Column(name = INodeStorage.IS_DIR)
  boolean getIsDir();

  void setIsDir(boolean isDir);

  // marker for InodeDirectoryWithQuota
  @Column(name = INodeStorage.IS_DIR_WITH_QUOTA)
  boolean getIsDirWithQuota();

  void setIsDirWithQuota(boolean isDirWithQuota);

  // Inode
  @Column(name = INodeStorage.MODIFICATION_TIME)
  long getModificationTime();

  void setModificationTime(long modificationTime);

  // Inode
  @Column(name = INodeStorage.ACCESS_TIME)
  long getATime();

  void setATime(long modificationTime);

  // Inode
  @Column(name = INodeStorage.PERMISSION)
  byte[] getPermission();

  void setPermission(byte[] permission);

  // InodeDirectoryWithQuota
  @Column(name = INodeStorage.NSCOUNT)
  long getNSCount();

  void setNSCount(long nsCount);

  // InodeDirectoryWithQuota
  @Column(name = INodeStorage.DSCOUNT)
  long getDSCount();

  void setDSCount(long dsCount);

  // InodeDirectoryWithQuota
  @Column(name = INodeStorage.NSQUOTA)
  long getNSQuota();

  void setNSQuota(long nsQuota);

  // InodeDirectoryWithQuota
  @Column(name = INodeStorage.DSQUOTA)
  long getDSQuota();

  void setDSQuota(long dsQuota);

  //  marker for InodeFileUnderConstruction
  @Column(name = INodeStorage.IS_UNDER_CONSTRUCTION)
  boolean getIsUnderConstruction();

  void setIsUnderConstruction(boolean isUnderConstruction);

  // InodeFileUnderConstruction
  @Column(name = INodeStorage.CLIENT_NAME)
  String getClientName();

  void setClientName(String isUnderConstruction);

  // InodeFileUnderConstruction
  @Column(name = INodeStorage.CLIENT_MACHINE)
  String getClientMachine();

  void setClientMachine(String clientMachine);

  @Column(name = INodeStorage.CLIENT_NODE)
  String getClientNode();

  void setClientNode(String clientNode);

  //  marker for InodeFile
  @Column(name = INodeStorage.IS_CLOSED_FILE)
  boolean getIsClosedFile();

  void setIsClosedFile(boolean isClosedFile);

  // InodeFile
  @Column(name = INodeStorage.HEADER)
  long getHeader();

  void setHeader(long header);

  //INodeSymlink
  @Column(name = INodeStorage.SYMLINK)
  String getSymlink();

  void setSymlink(String symlink);
}
