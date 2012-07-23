/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;

public class TestNamenodePing extends junit.framework.TestCase {

  static int countLease(final MiniDFSCluster cluster) {
    try {
      return (Integer) new TransactionalRequestHandler() {

        @Override
        public Object performTask() throws PersistanceException, IOException {
          return NameNodeAdapter.getLeaseManager(cluster.getNamesystem()).countLease();
        }
      }.handle();
    } catch (IOException ex) {
      Logger.getLogger(TestNamenodePing.class.getName()).log(Level.SEVERE, null, ex);
      return -1;
    }

  }
  final Path dir = new Path("/test/rename/");

  void list(FileSystem fs, String name) throws IOException {
    FileSystem.LOG.info("\n\n" + name);
    for (FileStatus s : fs.listStatus(dir)) {
      FileSystem.LOG.info("" + s.getPath());
    }
  }

  static void createFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream a_out = fs.create(f);
    a_out.writeBytes("something");
    a_out.close();
  }

  public void testPing() throws Exception {
    Configuration conf = new HdfsConfiguration();
    // Setting 3 reader namenodes
    //conf.set(DFSConfigKeys.DFS_READ_NAMENODES_RPC_ADDRESS_KEY, "hdfs://localhost:34243,hdfs://localhost:34244,hdfs://localhost:34245");
    // Not setting the writer namenode, it is the default namenode
    // conf.set(DFSConfigKeys.DFS_READ_NAMENODES_RPC_ADDRESS_KEY, "hdfs://localhost:34246");
    // Setting the selector policy
    conf.set(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, " org.apache.hadoop.hdfs.RoundRobinNameNodeSelector");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numRNameNodes(3).numWNameNodes(1).numDataNodes(2).build();
    cluster.waitActive();

    try {
      //FileSystem fs = cluster.getWritingFileSystem();
      FileSystem fs = cluster.getWritingFileSystem();
      assertTrue(fs.mkdirs(dir));

      {
        //test lease
        Path a = new Path(dir, "a");
        Path aa = new Path(dir, "aa");
        Path b = new Path(dir, "b");

        createFile(fs, a);

        //should not have any lease
        assertEquals(0, countLease(cluster));

        DataOutputStream aa_out = fs.create(aa);
        aa_out.writeBytes("something");

        //should have 1 lease
        assertEquals(1, countLease(cluster));
        list(fs, "rename0");
        fs.rename(a, b);
        list(fs, "rename1");
        aa_out.writeBytes(" more");
        aa_out.close();
        list(fs, "rename2");

        //should not have any lease
        assertEquals(0, countLease(cluster));

        // Shutting down all reader NNs but writer is still ON
        cluster.getReaderNameNode(0).stop();
        cluster.getReaderNameNode(1).stop();
        cluster.getReaderNameNode(2).stop();

        // We should expect to see retries in the logs
        // This should still work as now only the writer NN is doing the work of readers
        assertTrue(fs.exists(b));
        assertTrue(fs.exists(b));
        assertTrue(fs.exists(b));

        // Shutting down the writer NN. Now we have no NNs in the system
        cluster.getNameNode().stop();
        try {
          assertFalse(fs.exists(b));
        } catch (IOException ex) {
          //assertEquals(ex.getClass().getName(), java.net.ConnectException.class.getName());
          System.out.println("We got error! Because of no Reader/Writer NNs in the system. Exception: " + ex.getMessage());
          ex.printStackTrace();
        }
      }


      //fs.delete(dir, true);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
