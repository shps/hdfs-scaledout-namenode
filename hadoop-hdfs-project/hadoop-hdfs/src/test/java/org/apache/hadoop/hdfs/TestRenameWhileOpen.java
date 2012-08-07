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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;

public class TestRenameWhileOpen extends junit.framework.TestCase {

  public static final Log LOG = LogFactory.getLog(TestRenameWhileOpen.class);

  {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
  }

  //TODO: un-comment checkFullFile once the lease recovery is done
  private static void checkFullFile(FileSystem fs, Path p) throws IOException {
    //TestFileCreation.checkFullFile(fs, p);
  }

  /**
   * open /user/dir1/file1 /user/dir2/file2
   * mkdir /user/dir3
   * move /user/dir1 /user/dir3
   */
  public void testWhileOpenRenameParent() throws IOException {
    Configuration conf = new HdfsConfiguration();

    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    LOG.info("Min replication: " + conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, -1));
    conf.setBoolean("dfs.support.append", true);

    // create cluster
    LOG.info("Test 1*****************************");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    LOG.info("Total Datanodes for test: " + cluster.getDataNodes().size());
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();
      final int nnport = cluster.getNameNodePort();

      // create file1.
      Path dir1 = new Path("/user/a+b/dir1");
      Path file1 = new Path(dir1, "file1");   //user/a+b/dir1/file1
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      LOG.info("testFileCreationDeleteParent: "
              + "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();
      //stm1.close();

      // create file2.
      Path dir2 = new Path("/user/dir2");
      Path file2 = new Path(dir2, "file2");   //user/dir2/file2
      FSDataOutputStream stm2 = TestFileCreation.createFile(fs, file2, 1);
      LOG.info("testFileCreationDeleteParent: "
              + "Created file " + file2);
      TestFileCreation.writeFile(stm2);
      stm2.hflush();
      //stm2.close();

      // move dir1 while file1 is open
      Path dir3 = new Path("/user/dir3");
      fs.mkdirs(dir3);
      fs.rename(dir1, dir3);  // rename: /user/a+b/dir1 to  /user/dir3  ===> /user/a+b /   and /user/dir3/dir1/file1

      // create file3
      Path file3 = new Path(dir3, "file3");           //user/dir3/file3
      FSDataOutputStream stm3 = TestFileCreation.createFile(fs, file3, 1);
      TestFileCreation.writeFile(stm3);
      //stm3.close();

      try {
        Thread.sleep(20000);   // 2mins.
      } catch (InterruptedException e) {
      }


      // rename file3 to some bad name
      try {
        fs.rename(file3, new Path(dir3, "$ "));         // rename: /user/dir3/file3  to /user/dir3/$
      } catch (Exception e) {
        e.printStackTrace();
      }

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      LOG.info("Shutting down cluster[1]...");
      cluster.shutdown();
      try {
        Thread.sleep(2 * MAX_IDLE_TIME);
      } catch (InterruptedException e) {
      }
      LOG.info("Creating New miniDFS Cluster instance[1]...");
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      LOG.info("Total Datanodes for test: " + cluster.getDataNodes().size());
      LOG.info("New miniDFS Cluster instance created[1]...");
      cluster.waitActive();
      LOG.info("Wait time over[1]...");

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      LOG.info("Shutting down cluster[2]...");
      cluster.shutdown();
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      LOG.info("Total Datanodes for test: " + cluster.getDataNodes().size());

      cluster.waitActive();
      fs = cluster.getWritingFileSystem();

      Path newfile = new Path("/user/dir3/dir1", "file1");
      assertTrue(fs.exists(newfile));
      assertTrue(!fs.exists(file1));  //user/a+b/dir1/file1          ... will not exist since it dir1 was deleted on rename operation
      assertTrue(fs.exists(file2));   //user/dir2/file2                    ... will exist
      checkFullFile(fs, newfile);     //user/dir3/dir1/file1          ... will exist since dir1 was moved to dir3
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * open /user/dir1/file1 /user/dir2/file2
   * move /user/dir1 /user/dir3
   */
  public void testWhileOpenRenameParentToNonexistentDir() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    conf.setBoolean("dfs.support.append", true);
    LOG.info("Test 2************************************");

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();
      final int nnport = cluster.getNameNodePort();

      // create file1.
      Path dir1 = new Path("/user/dir1");
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      LOG.info("testFileCreationDeleteParent: "
              + "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();

      // create file2.
      Path dir2 = new Path("/user/dir2");
      Path file2 = new Path(dir2, "file2");
      FSDataOutputStream stm2 = TestFileCreation.createFile(fs, file2, 1);
      LOG.info("testFileCreationDeleteParent: "
              + "Created file " + file2);
      TestFileCreation.writeFile(stm2);
      stm2.hflush();

      // move dir1 while file1 is open
      Path dir3 = new Path("/user/dir3");
      fs.rename(dir1, dir3);

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(2 * MAX_IDLE_TIME);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();

      Path newfile = new Path("/user/dir3", "file1");
      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(file2));
      assertTrue(fs.exists(newfile));
      checkFullFile(fs, newfile);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * open /user/dir1/file1 
   * mkdir /user/dir2
   * move /user/dir1/file1 /user/dir2/
   */
  public void testWhileOpenRenameToExistentDirectory() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    conf.setBoolean("dfs.support.append", true);
    LOG.info("Test 3************************************");

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();
      final int nnport = cluster.getNameNodePort();

      // create file1.
      Path dir1 = new Path("/user/dir1");
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      LOG.info("testFileCreationDeleteParent: "
              + "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();

      Path dir2 = new Path("/user/dir2");
      fs.mkdirs(dir2);

      fs.rename(file1, dir2);

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(2 * MAX_IDLE_TIME);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();

      Path newfile = new Path("/user/dir2", "file1");
      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(newfile));
      checkFullFile(fs, newfile);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * open /user/dir1/file1 
   * move /user/dir1/file1 /user/dir2/
   */
  public void testWhileOpenRenameToNonExistentDirectory() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    conf.setBoolean("dfs.support.append", true);
    LOG.info("Test 4************************************");

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();
      final int nnport = cluster.getNameNodePort();

      // create file1.
      Path dir1 = new Path("/user/dir1");
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      LOG.info("testFileCreationDeleteParent: "
              + "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();

      Path dir2 = new Path("/user/dir2");

      fs.rename(file1, dir2);

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(2 * MAX_IDLE_TIME);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).wNameNodePort(nnport).format(false).build();
      cluster.waitActive();
      fs = cluster.getWritingFileSystem();

      Path newfile = new Path("/user", "dir2");
      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(newfile));
      checkFullFile(fs, newfile);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
