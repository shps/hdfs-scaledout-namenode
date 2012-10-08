package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageFileNameGenerator extends FileNameGenerator {

  private long dirCount;
  private final long BASE_PARENT_ID; // Base parent id
  private final long BASE_FILE_ID;
  private final long TOTAL_FILES; // total number of files

  public StorageFileNameGenerator(String baseDir, long totalFiles) {
    this(baseDir, DEFAULT_FILES_PER_DIRECTORY, totalFiles);
  }

  public StorageFileNameGenerator(String baseDir, int filesPerDir, long totalFiles) {
    super(baseDir, filesPerDir);
    this.BASE_PARENT_ID = baseDir.split("/").length - 1;
    this.TOTAL_FILES = totalFiles;
    this.BASE_FILE_ID = (TOTAL_FILES / filesPerDir) + BASE_PARENT_ID;
  }

  public void buildBaseDirs(String baseDir) {
    try {
      String[] dirs = baseDir.split("/");
      EntityManager.begin();
      for (int i = 1; i < dirs.length; i++) {
        addINodeDirectory(dirs[i], i, i - 1);
      }
      EntityManager.commit();
    } catch (Exception ex) {
      Logger.getLogger(StorageFileNameGenerator.class.getName()).log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected synchronized void reset() {
    super.reset();
    dirCount = 0L;
  }

  synchronized String getNextStoredFileName(String fileNamePrefix, short replication,
          long blockSize, String clientName) throws IOException, PersistanceException {
    long fNum = fileCount % filesPerDirectory;
    boolean newDir = false;
    if (fNum == 0) {
      currentDir = getNextDirName(fileNamePrefix + "Dir");
      dirCount++;
      newDir = true;
    }
    String fn = currentDir + "/" + fileNamePrefix + fileCount;
    fileCount++;
    String[] pathComponents = fn.split("/");
    String file = pathComponents[pathComponents.length - 1];

    long parentId;
    if (newDir) {
      String lastDir = pathComponents[pathComponents.length - 2];
      parentId = (long) (Math.ceil((double) dirCount / (float) filesPerDirectory) + BASE_PARENT_ID - 1);
      addINodeDirectory(lastDir, dirCount + BASE_PARENT_ID, parentId);
    }

    PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            UserGroupInformation.getCurrentUser().getGroupNames()[0], FsPermission.getDefault());
    parentId = dirCount + BASE_PARENT_ID;
    long aTime = System.currentTimeMillis();
    // Since there is no block and datanode, so client machine and DatanodeId shouldn't be required.
    INodeFile inode = new INodeFile(false, file.getBytes(), replication, aTime, blockSize, permission,
            clientName, "", new DatanodeID(""));
    addInode(inode, BASE_FILE_ID + fileCount, parentId);

    return fn;
  }

  public static void addINodeDirectory(String name, long inodeId, long parentId) throws IOException, PersistanceException {
    PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            UserGroupInformation.getCurrentUser().getGroupNames()[0], FsPermission.getDefault());
    INodeDirectory dir = new INodeDirectory(name, permission);
    addInode(dir, inodeId, parentId);
  }

  public static void addINodeFile(String fileName, long inodeId, long parentId) throws IOException, PersistanceException {
    PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            UserGroupInformation.getCurrentUser().getGroupNames()[0], FsPermission.getDefault());
    INodeFile inode = new INodeFile(false, fileName.getBytes(), (short) 1, 0L, 1, permission,
            "easy-generator", "", new DatanodeID(""));
    addInode(inode, inodeId, parentId);
  }

  public static void addInode(INode inode, long inodeId, long parentId) throws PersistanceException {
    inode.setId(inodeId);
    inode.setParentId(parentId);
    EntityManager.add(inode);
  }
}
