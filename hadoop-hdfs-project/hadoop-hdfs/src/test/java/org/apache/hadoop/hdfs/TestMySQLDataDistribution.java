package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;

/**
 *
 * @author jude
 */
public class TestMySQLDataDistribution extends junit.framework.TestCase {

  static final Log LOG = LogFactory.getLog(TestMySQLDataDistribution.class);
  static final String DIR = "/" + TestMySQLDataDistribution.class.getSimpleName() + "/";

  {
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }
  
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;
  
  // creates a file but does not close it
  public static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    LOG.info("createFile: Created " + name + " with " + repl + " replica.");
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  static void writeFile(FSDataOutputStream stm) throws IOException {
    writeFile(stm, fileSize);
  }

  //
  // writes specified bytes to file.
  //
  public static void writeFile(FSDataOutputStream stm, int size) throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, size);
    stm.write(buffer, 0, size);
  }

  /**
   * Test if file creation and disk space consumption works right
   */
  public void testFileCreation() throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numNameNodes(1).build();
    FileSystem fs = cluster.getFileSystem();
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      LOG.info("Path : \"" + path.toString() + "\"");
      LOG.info(fs.getFileStatus(path).isDirectory()); 
      assertTrue("/ should be a directory", fs.getFileStatus(path).isDirectory());

      //
      // Create a directory inside /, then try to overwrite it
      //
      Path dir1 = new Path("/test_dir");
      fs.mkdirs(dir1);
      
      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("filestatus.dat");
      Path parent = file1.getParent();
      fs.mkdirs(parent);
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      dfs.setQuota(file1.getParent(), 100L, blockSize*5);
      FSDataOutputStream stm = createFile(fs, file1, 1);

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", fs.getFileStatus(file1).isFile());
      LOG.info("Path : \"" + file1 + "\"");

      // write to file
      writeFile(stm);

      stm.close();

    } finally {
      cluster.shutdown();
    }
  }
  
}
