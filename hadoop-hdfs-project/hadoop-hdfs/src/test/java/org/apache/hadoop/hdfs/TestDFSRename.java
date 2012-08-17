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
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler.OperationType;

public class TestDFSRename extends junit.framework.TestCase {

  static int countLease(final MiniDFSCluster cluster) {
    try {
      return (Integer) new TransactionalRequestHandler(OperationType.COUNT_LEASE_DFS_RENAME) {

        @Override
        public Object performTask() throws PersistanceException, IOException {
          return NameNodeAdapter.getLeaseManager(cluster.getNamesystem()).countLease();
        }
      }.handle();
    } catch (IOException ex) {
      Logger.getLogger(TestDFSRename.class.getName()).log(Level.SEVERE, null, ex);
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

  public void testRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      assertTrue(fs.mkdirs(dir));
      { //test lease
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
      }
      { // test non-existent destination
        Path dstPath = new Path("/c/d");
        assertFalse(fs.exists(dstPath));
        assertFalse(fs.rename(dir, dstPath));
      }
      { // dst cannot be a file or directory under src
        // test rename /a/b/foo to /a/b/c
        Path src = new Path("/a/b");
        Path dst = new Path("/a/b/c");

        createFile(fs, new Path(src, "foo"));

        // dst cannot be a file under src
        assertFalse(fs.rename(src, dst));

        // dst cannot be a directory under src
        assertFalse(fs.rename(src.getParent(), dst.getParent()));
      }
      { // dst can start with src, if it is not a directory or file under src
        // test rename /test /testfile
        Path src = new Path("/testPrefix");
        Path dst = new Path("/testPrefixfile");

        createFile(fs, src);
        assertTrue(fs.rename(src, dst));
      }
      { // dst should not be same as src test rename /a/b/c to /a/b/c
        Path src = new Path("/a/b/c");
        createFile(fs, src);
        assertTrue(fs.rename(src, src));
        assertFalse(fs.rename(new Path("/a/b"), new Path("/a/b/")));
        assertTrue(fs.rename(src, new Path("/a/b/c/")));
      }
      {
        // Create:                              /user/a+b/dir1
        // Add file1 to dir1 :         /user/a+b/dir1/file1
        // Create:                              /user/dir2/
        // Rename:                          /user/a+b/dir1     to      /user/dir2
        //                                               Result:        /user/a+b/   and  /user/dir2/file1              OR   /user/dir2/dir1/file1
        // Add file2 to dir2          /user/dir2/file2
        Path dir1 = new Path("/user/a+b/dir1");
        Path file1 = new Path(dir1, "file1");
        fs.mkdirs(dir1);
        createFile(fs, file1);

        Path dir2 = new Path("/user/dir2");
        fs.mkdirs(dir2);
        assertTrue(fs.rename(dir1, dir2));              // dir2 exists, so dir1 will be moved inside of dir2
        // This would result in /user/a+b/..     and /user/dir2/dir1/file1

        Path file2 = new Path(dir2, "file2");
        createFile(fs, new Path(dir2, file2));
        assertTrue(fs.exists(file2));           // user/dir2/file2 should exist
        assertFalse(fs.exists(file1));          // user/a+b/dir1 was deleted via rename
        assertFalse(fs.exists(dir1));           // user/a+b/dir1 was deleted via rename

      }
      fs.delete(dir, true);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
