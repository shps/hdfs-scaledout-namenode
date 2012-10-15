package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.persistance.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.security.UserGroupInformation;


/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageFileNameGenerator extends FileNameGenerator {

  private long dirCount;
  private final long BASE_DIR_COUNT;
  private final long BASE_PARENT_ID; // Base parent id
  private final long BASE_FILE_ID;
  private final long TOTAL_FILES; // total number of files
  private Configuration config;
  EntityManager em = EntityManager.getInstance();
  
//  public StorageFileNameGenerator(String baseDir, long totalFiles, Configuration config) {
//    this(baseDir, DEFAULT_FILES_PER_DIRECTORY, totalFiles, config);
//  }

  public StorageFileNameGenerator(String baseDir, int filesPerDir, long totalFiles, Configuration config, long namenodeId) {
    super(baseDir, filesPerDir);
    //this.BASE_PARENT_ID = baseDir.split("/").length - 1;
    /**
     * nn1:   0 * 11 + 2 = 2    ~~ 12   ==> [1, 2],[3,4][5~~ 14]
     * nn2:   1 * 11 + 2 = 13 ~~ 23   ==> [12,13], [14,15][15~~ 25]
     * nn3:   2 * 11 + 2 = 24 ~~ 34
     */
    /** Each namenode would require space of:
     * Two base directories                                                       /nnThroughputBenchmark__1/open                                ==> 2
     * (TotalFiles/filesPerDir) # of directories              2 directories (if 5 files per directory for 10 files)           ==> 2
     * TotalFiles                                                                                10 files                                                                                                     ==> 10
     *                                                                                                                                                                                                                      --------------
     * Total entries (2BaseDir+[TotalFiles/filesPerDir]+TotalFiles)                                                                                              14 entries in total ranging from [1-14]
     * 
     * Next namenode would begin
     *          [15-29].... (14 entries per namenode)
     *          [30-44].... (14 entries per namenode)
     *          [45-59].... (14 entries per namenode)
     * 
     * This looks like a multiple of 15... 
     * 
     * Factor: 2+(10,000/5,000)+(10,000) = 10,004
     * --------------------------------------------------
     * NN1 ----->  BASE_PARENT_ID: 0x10,004  ==> [1-->/nnThroughputBenchmark__1] , [2-->/open] , [3-->dir1] , [4-->dir2] , [5~~10,004]
     * NN2 ----->  BASE_PARENT_ID: 1x10,004  ==> [10,005-->/nnThroughputBenchmark__1] , [10,006-->/open] , [10,007-->dir1] , [10,008-->dir2] , [10,009~~20,008]
     */
    //this.BASE_PARENT_ID = ((namenodeId-1) * (totalFiles+1))+baseDir.split("/").length - 1;  
    long factor = (baseDir.split("/").length - 1)+(totalFiles/filesPerDir) + totalFiles +1;     
    this.BASE_DIR_COUNT = baseDir.split("/").length - 1;
    this.BASE_PARENT_ID = ((namenodeId-1) * factor) ;
    this.TOTAL_FILES = totalFiles;
    this.BASE_FILE_ID =(TOTAL_FILES / filesPerDir) + BASE_PARENT_ID+BASE_DIR_COUNT;
    System.out.println("BASE_PARENT_ID: "+BASE_PARENT_ID+", BASE_FILE_ID: "+BASE_FILE_ID + ", NNid: "+namenodeId);
    this.config = config;
    DBConnector.setConfiguration(config);
  }

  public void buildBaseDirs(String baseDir, FSDirectory dir) {
    try {
      String[] dirs = baseDir.split("/");
      int base = (int) BASE_PARENT_ID;
      for (int i = 1; i < dirs.length; i++) {
    PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            UserGroupInformation.getCurrentUser().getGroupNames()[0], FsPermission.getDefault());
        //addINodeDirectory(dirs[i], i, i - 1);
    
        if(i-1 == 0) {
          // parent == "/"  --> 0
          // child      == "/nnThroughputBenchmark__1"
          addINodeDirectory(dirs[i], base +i, 0);
        }
        else {
          addINodeDirectory(dirs[i], base +i, base+i - 1);
        }
      }
      //addINodeDirectory(baseDir, dir);
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
          long blockSize, String clientName, FSDirectory dir) throws IOException {
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
      parentId = (long) (Math.ceil((double)dirCount / (float)filesPerDirectory) + BASE_DIR_COUNT + BASE_PARENT_ID - 1);
      addINodeDirectory(lastDir, dirCount + BASE_DIR_COUNT + BASE_PARENT_ID, parentId);
      //addINodeDirectory(fn, dir);
    }

    PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            UserGroupInformation.getCurrentUser().getGroupNames()[0], FsPermission.getDefault());
    parentId = dirCount + BASE_DIR_COUNT + BASE_PARENT_ID;
    long aTime = System.currentTimeMillis();
    // Since there is no block and datanode, so client machine and DatanodeId shouldn't be required.
    INodeFile inode = new INodeFile(permission, replication, aTime, aTime, blockSize);
    inode.setLocalName(file);
    addInode(inode, BASE_FILE_ID + fileCount, parentId);
    //dir.addFile(fn, permission, replication, blockSize, clientName, clientName, null, aTime, true);
    return fn;
  }

  private void addINodeDirectory(String name, long inodeId, long parentId) throws IOException {
    PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            UserGroupInformation.getCurrentUser().getGroupNames()[0], FsPermission.getDefault());
    INodeDirectory dir = new INodeDirectory(name, permission);
    addInode(dir, inodeId, parentId);
  }
  
//  private void addINodeDirectory(String path, FSDirectory dir) throws IOException{
//    PermissionStatus permission = new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
//            UserGroupInformation.getCurrentUser().getGroupNames()[0], FsPermission.getDefault());
//    dir.mkdirs(path, permission, true, System.currentTimeMillis(), true);
//  }

  private void addInode(INode inode, long inodeId, long parentId) {
    
    inode.setID(inodeId);
    inode.setParentIDLocal(parentId);
    INodeHelper.addChild(inode, false, parentId, true);
  }
}