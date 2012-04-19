package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * Test to validate the getBlockLocations functionality in the readerNN
 * 
 * Tests the following functionality:
 * 1. Start a writerNN with datanodes
 * 2. Start a readerNN without datanodes
 * 3. Write a file (with blocks) on the writerNN and the datanodes
 * 4. Read that file from the readerNN (with tokens disabled)
 * 5. Read that file from the readerNN (with tokens enabled)
 * 
 * @author wmalik
 */
public class TestGetBlockLocations {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 2 * BLOCK_SIZE;
  private final byte[] rawData = new byte[FILE_SIZE];

  {
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    Random r = new Random();
    r.nextBytes(rawData);
  }

  private void createFile(FileSystem fs, Path filename) throws IOException {
    FSDataOutputStream out = fs.create(filename);
    out.write(rawData);
    out.close();
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

  // read a file using fetchBlockByteRange()
  private boolean checkFile2(FSDataInputStream in) {
    byte[] toRead = new byte[FILE_SIZE];
    try {
      assertEquals("Cannot read file", toRead.length, in.read(0, toRead, 0,
          toRead.length));
    } catch (IOException e) {
      return false;
    }
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


  // get a conf for testing
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
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setBoolean("dfs.support.append", true);
    conf.setStrings(DFSConfigKeys.DFS_DB_DATABASE_KEY, "kthfs-getblocks");
    return conf;
  }
  
  private MiniDFSCluster startCluster(boolean tokens) throws IOException {
	    MiniDFSCluster cluster = null;
	    int numDataNodes = 2;
	    Configuration conf = getConf(numDataNodes, tokens);
	    DBConnector.setConfiguration(conf);
	    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
	    cluster.waitActive();
	    assertEquals(numDataNodes, cluster.getDataNodes().size());
	    return cluster;
}
  
   
  @Test
  public void testReadNnWithoutTokens() throws Exception{
	  
	  MiniDFSCluster cluster = null;
	  try {
      cluster = startCluster(false);
      FileSystem writeFs = cluster.getWritingFileSystem();
      FileSystem readFs = cluster.getReadingFileSystem();

      //create some directories
      Path filePath1 = new Path("/testDir1");
      Path filePath2 = new Path("/testDir2");
      Path filePath3 = new Path("/testDir3");
      assertTrue(writeFs.mkdirs(filePath1));
      assertTrue(writeFs.mkdirs(filePath2));
      assertTrue(writeFs.mkdirs(filePath3));
      
      //check if the directories were created successfully
      FileStatus lfs = writeFs.getFileStatus(filePath1);
      assertTrue(lfs.getPath().getName().equals("testDir1"));
      
      //write a new file on the writeNn and confirm if it can be read
      Path fileTxt = new Path("/file.txt");
      createFile(writeFs, fileTxt);
      FSDataInputStream in1 = writeFs.open(fileTxt);
      assertTrue(checkFile1(in1)); 

      //try to read the file from the readNn (calls readerFsNamesystem.getBlockLocations under the hood) 
      FSDataInputStream in2 = readFs.open(fileTxt);
	  assertTrue(checkFile1(in2));
	  
	  readFs.close();
	  writeFs.close();
	  } finally {
		  if (cluster != null) {
			  cluster.shutdown();
		  }
	  }


  }
  
  //TODO: Currently, it is not possible to start a MiniDFSCluster after a shutdown
  //      because of the following error:
  //      java.io.IOException: Cannot lock storage 
  //      /home/wmalik/hadoopnn/kthfs/hadoop-hdfs-project/hadoop-hdfs/target/test/data/dfs/rName1. 
  //      The directory is already locked.
  //@Test
  public void testReadNnWithTokens() throws Exception{
	  
	  MiniDFSCluster cluster = null;
	  try {
      cluster = startCluster(true); //use tokens
      FileSystem writeFs = cluster.getWritingFileSystem();
      FileSystem readFs = cluster.getReadingFileSystem();

      //create some directories
      Path filePath1 = new Path("/testDir1");
      Path filePath2 = new Path("/testDir2");
      Path filePath3 = new Path("/testDir3");
      assertTrue(writeFs.mkdirs(filePath1));
      assertTrue(writeFs.mkdirs(filePath2));
      assertTrue(writeFs.mkdirs(filePath3));
      
      //check if the directories were created successfully
      FileStatus lfs = writeFs.getFileStatus(filePath1);
      assertTrue(lfs.getPath().getName().equals("testDir1"));
      
      //write a new file on the writeNn and confirm if it can be read
      Path fileTxt = new Path("/file.txt");
      createFile(writeFs, fileTxt);
      FSDataInputStream in1 = writeFs.open(fileTxt);
      assertTrue(checkFile1(in1)); 

      //try to read the file from the readNn (calls readerFsNamesystem.getBlockLocations under the hood) 
      FSDataInputStream in2 = readFs.open(fileTxt);
	  assertTrue(checkFile1(in2));

	  } finally {
		  if (cluster != null) {
			  cluster.shutdown();
		  }
	  }


  }

}
