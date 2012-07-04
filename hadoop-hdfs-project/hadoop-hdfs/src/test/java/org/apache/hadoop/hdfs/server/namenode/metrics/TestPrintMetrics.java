package org.apache.hadoop.hdfs.server.namenode.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.junit.Test;

/**
 * This test prints out the database round trips for the following operations:
 *  - create
 *  - mkdir
 *  - ls
 *  - getFileStatus
 *  - getBLockLocations
 *  - rename
 *  - delete
 *   
 * @author wmalik
 *
 */
public class TestPrintMetrics {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 1 * BLOCK_SIZE;
  private final byte[] rawData = new byte[FILE_SIZE];

  {
    Random r = new Random();
    r.nextBytes(rawData);
  }

  // read a file using blockSeekTo()
  private boolean checkFile1(FSDataInputStream in) {
    byte[] toRead = new byte[FILE_SIZE];
    int totalRead = 0;
    int nRead = 0;
    try {
      while ((nRead = in.read(toRead, totalRead, toRead.length - totalRead)) > 0) {
        totalRead += nRead;
      }
    } catch (IOException e) {
      e.printStackTrace(System.err);
      return false;
    }
    assertEquals("Cannot read file.", toRead.length, totalRead);
    return checkFile(toRead);
  }


  private boolean checkFile(byte[] fileToCheck) {
    if (fileToCheck.length != rawData.length) {
      return false;
    }
    for (int i = 0; i < fileToCheck.length; i++) {
      if (fileToCheck[i] != rawData[i]) {
        return false;
      }
    }
    return true;
  }


  /**
   * @param numDataNodes
   * @param tokens enable tokens?
   * @return
   * @throws IOException
   */
  private static Configuration getConf(int numDataNodes, boolean tokens) throws IOException {
    Configuration conf = new Configuration();
    if(tokens)
      conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt("io.bytes.per.checksum", BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1500);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setBoolean("dfs.support.append", true);
    conf.setStrings(DFSConfigKeys.DFS_DB_DATABASE_KEY, DFSConfigKeys.DFS_DB_DATABASE_DEFAULT);
    return conf;
  }

  private MiniDFSCluster startCluster(boolean tokens) throws IOException {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    Configuration conf = getConf(numDataNodes, tokens);
    StorageFactory.getConnector().setConfiguration(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
    cluster.waitActive();
    assertEquals(numDataNodes, cluster.getDataNodes().size());
    return cluster;
  }


  @Test
  public void testPrintMetrics() throws Exception{

    MiniDFSCluster cluster = null;
    try {
      cluster = startCluster(false);
      FileSystem fs = cluster.getWritingFileSystem();

      //create a directory
      Path filePath1 = new Path("/testDir1");
      assertTrue(fs.mkdirs(filePath1));
      HelperMetrics.printAllAndReset("mkdir");

      //ls the directory
      fs.listFiles(filePath1, false);
      HelperMetrics.printAllAndReset("listing");

      //get status of the directory
      FileStatus lfs = fs.getFileStatus(filePath1);
      assertTrue(lfs.getPath().getName().equals("testDir1"));
      HelperMetrics.printAllAndReset("getFileStatus");

      //write a new file of size BLOCK_LENGTH*2
      Path fileTxt = new Path("/file.txt");
      FSDataOutputStream out = fs.create(fileTxt);
      HelperMetrics.printAllAndReset("create file");
      out.write(rawData);
      out.close();
      Thread.sleep(5*1000); //allow the datanodes to send block reports
      HelperMetrics.printAllAndReset("addBlock (num_blocks="+FILE_SIZE/BLOCK_SIZE+")");


      //get block locations
      FSDataInputStream in1 = fs.open(fileTxt);
      HelperMetrics.printAllAndReset("getBlockLocations");
      assertTrue(checkFile1(in1));

      //rename
      Path file_bkp = new Path("/file_bkp.txt");
      fs.rename(fileTxt, file_bkp);
      HelperMetrics.printAllAndReset("rename");

      //delete file with blocks
      fs.delete(fileTxt, false);
      HelperMetrics.printAllAndReset("delete file with "+FILE_SIZE/BLOCK_SIZE + " blocks");


      //delete directory
      fs.delete(filePath1, false);
      HelperMetrics.printAllAndReset("delete directory");

      fs.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
