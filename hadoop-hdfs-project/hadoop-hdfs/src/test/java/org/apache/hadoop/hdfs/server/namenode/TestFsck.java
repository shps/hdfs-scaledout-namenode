/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.security.PrivilegedExceptionAction;
import java.util.Random;
import java.util.regex.Pattern;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * A JUnit test for doing fsck
 */
public class TestFsck extends TestCase {
  static final Log LOG = LogFactory.getLog(TestFsck.class);
  static final String auditLogFile = System.getProperty("test.build.dir",
      "build/test") + "/audit.log";
  
  // Pattern for: 
  // ugi=name ip=/address cmd=FSCK src=/ dst=null perm=null
  static final Pattern fsckPattern = Pattern.compile(
      "ugi=.*?\\s" + 
      "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + 
      "cmd=fsck\\ssrc=\\/\\sdst=null\\s" + 
      "perm=null");
  
  static String runFsck(Configuration conf, int expectedErrCode, 
                        boolean checkErrorCode,String... path) 
                        throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    ((Log4JLogger)FSPermissionChecker.LOG).getLogger().setLevel(Level.ALL);
    int errCode = ToolRunner.run(new DFSck(conf, out), path);
    if (checkErrorCode)
      assertEquals(expectedErrCode, errCode);
    ((Log4JLogger)FSPermissionChecker.LOG).getLogger().setLevel(Level.INFO);
    return bStream.toString();
  }

  /** do fsck */
  public void testFsck() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 20, 3, 8*1024);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final long precision = 1L;
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, precision);
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      fs = cluster.getWritingFileSystem();
      final String fileName = "/srcdat";
      util.createFiles(fs, fileName);
      util.waitReplication(fs, fileName, (short)3);
      final Path file = new Path(fileName);
      long aTime = fs.getFileStatus(file).getAccessTime();
      Thread.sleep(precision);
      setupAuditLogs();
      String outStr = runFsck(conf, 0, true, "/");
      verifyAuditLogs();
      assertEquals(aTime, fs.getFileStatus(file).getAccessTime());
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      LOG.info(outStr);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
      
      // restart the cluster; bring up namenode but not the data nodes
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).format(false).build();
      outStr = runFsck(conf, 1, true, "/");
      // expect the result is corrupt
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
      LOG.info(outStr);
      
      // bring up data nodes & cleanup cluster
      cluster.startDataNodes(conf, 4, true, null, null);
      cluster.waitActive();
      cluster.waitClusterUp();
      fs = cluster.getWritingFileSystem();
      util.cleanup(fs, "/srcdat");
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** Sets up log4j logger for auditlogs */
  private void setupAuditLogs() throws IOException {
    File file = new File(auditLogFile);
    if (file.exists()) {
      file.delete();
    }
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.INFO);
    PatternLayout layout = new PatternLayout("%m%n");
    RollingFileAppender appender = new RollingFileAppender(layout, auditLogFile);
    logger.addAppender(appender);
  }
  
  private void verifyAuditLogs() throws IOException {
    // Turn off the logs
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.OFF);
    
    // Ensure audit log has only one for FSCK
    BufferedReader reader = new BufferedReader(new FileReader(auditLogFile));
    String line = reader.readLine();
    assertNotNull(line);
    assertTrue("Expected fsck event not found in audit log",
        fsckPattern.matcher(line).matches());
    assertNull("Unexpected event in audit log", reader.readLine());
  }
  
  public void testFsckNonExistent() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 20, 3, 8*1024);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      fs = cluster.getWritingFileSystem();
      util.createFiles(fs, "/srcdat");
      util.waitReplication(fs, "/srcdat", (short)3);
      String outStr = runFsck(conf, 0, true, "/non-existent");
      assertEquals(-1, outStr.indexOf(NamenodeFsck.HEALTHY_STATUS));
      LOG.info(outStr);
      util.cleanup(fs, "/srcdat");
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** Test fsck with permission set on inodes */
  public void testFsckPermission() throws Exception {
    final DFSTestUtil util = new DFSTestUtil(getClass().getSimpleName(), 20, 3, 8*1024);
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);

    MiniDFSCluster cluster = null;
    try {
      // Create a cluster with the current user, write some files
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      final MiniDFSCluster c2 = cluster;
      final String dir = "/dfsck";
      final Path dirpath = new Path(dir);
      final FileSystem fs = c2.getWritingFileSystem();

      util.createFiles(fs, dir);
      util.waitReplication(fs, dir, (short) 3);
      fs.setPermission(dirpath, new FsPermission((short) 0700));

      // run DFSck as another user, should fail with permission issue
      UserGroupInformation fakeUGI = UserGroupInformation.createUserForTesting(
          "ProbablyNotARealUserName", new String[] { "ShangriLa" });
      fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          LOG.info(runFsck(conf, -1, true, dir));
          return null;
        }
      });
      
      // set permission and try DFSck again as the fake user, should succeed
      fs.setPermission(dirpath, new FsPermission((short) 0777));
      fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final String outStr = runFsck(conf, 0, true, dir);
          LOG.info(outStr);
          assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
          return null;
        }
      });

      util.cleanup(fs, dir);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testFsckMove() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 5, 3, 8*1024);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      String topDir = "/srcdat";
      fs = cluster.getWritingFileSystem();
      cluster.waitActive();
      util.createFiles(fs, topDir);
      util.waitReplication(fs, topDir, (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      
      // Corrupt a block by deleting it
      String[] fileNames = util.getFileNames(topDir);
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                          cluster.getNameNodePort()), conf);
      ExtendedBlock block = dfsClient.getNamenode().getBlockLocations(
          fileNames[0], 0, Long.MAX_VALUE).get(0).getBlock();
      for (int i=0; i<4; i++) {
        File blockFile = MiniDFSCluster.getBlockFile(i, block);
        if(blockFile != null && blockFile.exists()) {
          assertTrue(blockFile.delete());
        }
      }

      // We excpect the filesystem to be corrupted
      outStr = runFsck(conf, 1, false, "/");
      while (!outStr.contains(NamenodeFsck.CORRUPT_STATUS)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }
        outStr = runFsck(conf, 1, false, "/");
      } 
      
      // Fix the filesystem by moving corrupted files to lost+found
      outStr = runFsck(conf, 1, true, "/", "-move");
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
      
      // Check to make sure we have healthy filesystem
      outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS)); 
      util.cleanup(fs, topDir);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  public void testFsckOpenFiles() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 4, 3, 8*1024); 
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      String topDir = "/srcdat";
      String randomString = "HADOOP  ";
      fs = cluster.getWritingFileSystem();
      cluster.waitActive();
      util.createFiles(fs, topDir);
      util.waitReplication(fs, topDir, (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      // Open a file for writing and do not close for now
      Path openFile = new Path(topDir + "/openFile");
      FSDataOutputStream out = fs.create(openFile);
      int writeCount = 0;
      while (writeCount != 100) {
        out.write(randomString.getBytes());
        writeCount++;                  
      }
      // We expect the filesystem to be HEALTHY and show one open file
      outStr = runFsck(conf, 0, true, topDir);
      LOG.info(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertFalse(outStr.contains("OPENFORWRITE")); 
      // Use -openforwrite option to list open files
      outStr = runFsck(conf, 0, true, topDir, "-openforwrite");
      LOG.info(outStr);
      assertTrue(outStr.contains("OPENFORWRITE"));
      assertTrue(outStr.contains("openFile"));
      // Close the file
      out.close(); 
      // Now, fsck should show HEALTHY fs and should not show any open files
      outStr = runFsck(conf, 0, true, topDir);
      LOG.info(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertFalse(outStr.contains("OPENFORWRITE"));
      util.cleanup(fs, topDir);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testCorruptBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    Random random = new Random();
    String outStr = null;
    short factor = 1;

    MiniDFSCluster cluster = null;
    try {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getWritingFileSystem();
    Path file1 = new Path("/testCorruptBlock");
    DFSTestUtil.createFile(fs, file1, 1024, factor, 0);
    // Wait until file replication has completed
    DFSTestUtil.waitReplication(fs, file1, factor);
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file1);

    // Make sure filesystem is in healthy state
    outStr = runFsck(conf, 0, true, "/");
    LOG.info(outStr);
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    
    // corrupt replicas
    File blockFile = MiniDFSCluster.getBlockFile(0, block);
    if (blockFile != null && blockFile.exists()) {
      RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
      FileChannel channel = raFile.getChannel();
      String badString = "BADBAD";
      int rand = random.nextInt((int) channel.size()/2);
      raFile.seek(rand);
      raFile.write(badString.getBytes());
      raFile.close();
    }
    // Read the file to trigger reportBadBlocks
    try {
      IOUtils.copyBytes(fs.open(file1), new IOUtils.NullOutputStream(), conf,
                        true);
    } catch (IOException ie) {
      // Ignore exception
    }

    dfsClient = new DFSClient(new InetSocketAddress("localhost",
                               cluster.getNameNodePort()), conf);
    blocks = dfsClient.getNamenode().
               getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    replicaCount = blocks.get(0).getLocations().length;
    while (replicaCount != factor) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
      blocks = dfsClient.getNamenode().
                getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      replicaCount = blocks.get(0).getLocations().length;
    }
    assertTrue (blocks.get(0).isCorrupt());

    // Check if fsck reports the same
    outStr = runFsck(conf, 1, true, "/");
    LOG.info(outStr);
    assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
    assertTrue(outStr.contains("testCorruptBlock"));
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /** Test if fsck can return -1 in case of failure
   * 
   * @throws Exception
   */
  public void testFsckError() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      // bring up a one-node cluster
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      String fileName = "/test.txt";
      Path filePath = new Path(fileName);
      FileSystem fs = cluster.getWritingFileSystem();
      
      // create a one-block file
      DFSTestUtil.createFile(fs, filePath, 1L, (short)1, 1L);
      DFSTestUtil.waitReplication(fs, filePath, (short)1);
      
      // intentionally corrupt NN data structure
      INodeFile node = 
        (INodeFile)cluster.getNamesystem().dir.rootDir.getNode(fileName,
                                                               true);
      assertEquals(node.getBlocks().size(), 1);
      node.getBlocks().get(0).setNumBytes(-1L);  // set the block length to be negative
      
      // run fsck and expect a failure with -1 as the error code
      String outStr = runFsck(conf, -1, true, fileName);
      LOG.info(outStr);
      assertTrue(outStr.contains(NamenodeFsck.FAILURE_STATUS));
      
      // clean up file system
      fs.delete(filePath, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /** check if option -list-corruptfiles of fsck command works properly */
  public void testFsckListCorruptFilesBlocks() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
    FileSystem fs = null;

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();
      DFSTestUtil util = new DFSTestUtil("testGetCorruptFiles", 3, 1, 1024);
      util.createFiles(fs, "/corruptData", (short) 1);
      util.waitReplication(fs, "/corruptData", (short) 1);

      // String outStr = runFsck(conf, 0, true, "/corruptData", "-list-corruptfileblocks");
      String outStr = runFsck(conf, 0, false, "/corruptData", "-list-corruptfileblocks");
      LOG.info("1. good fsck out: " + outStr);
      assertTrue(outStr.contains("has 0 CORRUPT files"));
      // delete the blocks
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      for (int i=0; i<4; i++) {
        for (int j=0; j<=1; j++) {
          File storageDir = MiniDFSCluster.getStorageDir(i, j);
          File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
          File[] blocks = data_dir.listFiles();
          if (blocks == null)
            continue;
  
          for (int idx = 0; idx < blocks.length; idx++) {
            if (!blocks[idx].getName().startsWith("blk_")) {
              continue;
            }
            assertTrue("Cannot remove file.", blocks[idx].delete());
          }
        }
      }

      // wait for the namenode to see the corruption
      final NamenodeProtocols namenode = cluster.getNameNodeRpc();
      CorruptFileBlocks corruptFileBlocks = namenode
          .listCorruptFileBlocks("/corruptData", null);
      int numCorrupt = corruptFileBlocks.getFiles().length;
      while (numCorrupt == 0) {
        Thread.sleep(1000);
        corruptFileBlocks = namenode
            .listCorruptFileBlocks("/corruptData", null);
        numCorrupt = corruptFileBlocks.getFiles().length;
      }
      outStr = runFsck(conf, -1, true, "/corruptData", "-list-corruptfileblocks");
      LOG.info("2. bad fsck out: " + outStr);
      assertTrue(outStr.contains("has 3 CORRUPT files"));

      // Do a listing on a dir which doesn't have any corrupt blocks and validate
      util.createFiles(fs, "/goodData");
      outStr = runFsck(conf, 0, true, "/goodData", "-list-corruptfileblocks");
      LOG.info("3. good fsck out: " + outStr);
      assertTrue(outStr.contains("has 0 CORRUPT files"));
      util.cleanup(fs,"/corruptData");
      util.cleanup(fs, "/goodData");
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Test for checking fsck command on illegal arguments should print the proper
   * usage.
   */
  public void testToCheckTheFsckCommandOnIllegalArguments() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      // bring up a one-node cluster
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      String fileName = "/test.txt";
      Path filePath = new Path(fileName);
      FileSystem fs = cluster.getWritingFileSystem();

      // create a one-block file
      DFSTestUtil.createFile(fs, filePath, 1L, (short) 1, 1L);
      DFSTestUtil.waitReplication(fs, filePath, (short) 1);

      // passing illegal option
      String outStr = runFsck(conf, -1, true, fileName, "-thisIsNotAValidFlag");
      LOG.info(outStr);
      assertTrue(!outStr.contains(NamenodeFsck.HEALTHY_STATUS));

      // passing multiple paths are arguments
      outStr = runFsck(conf, -1, true, "/", fileName);
      LOG.info(outStr);
      assertTrue(!outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      // clean up file system
      fs.delete(filePath, true);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
