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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestGetBlockLocations {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 2 * BLOCK_SIZE;
  private static final String FILE_TO_READ = "/fileToRead.dat";
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
    return conf;
  }
  
  private MiniDFSCluster startWriteCluster(boolean tokens) throws IOException {
	    MiniDFSCluster cluster = null;
	    int numDataNodes = 2;
	    Configuration conf = getConf(numDataNodes, tokens);
	    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
	    cluster.waitActive();
	    assertEquals(numDataNodes, cluster.getDataNodes().size());
	    return cluster;
  }
  
  private MiniDFSCluster startReadCluster(boolean tokens) throws IOException {
	  MiniDFSCluster readCluster = null;
	  int numDataNodes = 0;
	  Configuration conf = getConf(numDataNodes, tokens);
	  readCluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
	  readCluster.waitActive();
	  assertEquals(numDataNodes, readCluster.getDataNodes().size());
	  return readCluster;
  }
  
  /**
   * Tests the following functionality:
   * 1. Start a writerNN with datanodes
   * 2. Start a readerNN without datanodes
   * 3. Write a file (with blocks) on the writerNN and the datanodes
   * 4. Read that file from the readerNN (with tokens disabled)
   * 5. Read that file from the readerNN (with tokens enabled)
 * @throws Exception
 */
@SuppressWarnings("unused")
  @Test
  public void testReadNnWithoutTokens() throws Exception{
	  
	  MiniDFSCluster readCluster = null;
	  MiniDFSCluster writeCluster = null;
	  try {
	  
      
      writeCluster = startWriteCluster(false);
      final NameNode writeNn = writeCluster.getNameNode();
      final NamenodeProtocols writeNnProto = writeNn.getRpcServer();
      FileSystem writeFs = writeCluster.getNewFileSystemInstance(0);

      Path filePath1 = new Path("/testDir1");
      Path filePath2 = new Path("/testDir2");
      Path filePath3 = new Path("/testDir3");
      assertTrue(writeFs.mkdirs(filePath1));
      assertTrue(writeFs.mkdirs(filePath2));
      assertTrue(writeFs.mkdirs(filePath3));
      
      byte[] b = new byte[1];
      DirectoryListing dl = writeNnProto.getListing("/", b, false);
      HdfsFileStatus[] hfs = dl.getPartialListing();
      assertEquals(3, hfs.length);
      
      
      //write a new file on the writeNn and confirm if it can be read
      Path fileTxt = new Path("/file.txt");
      createFile(writeFs, fileTxt);
      FSDataInputStream in1 = writeFs.open(fileTxt);
      assertTrue(checkFile1(in1)); 
      
      //closing writeFs to make sure writeNn doesnt exist (paranoia)
      writeFs.close();
      //FileSystem.closeAll(); //not required
      writeCluster.shutdown();
      //readCluster.shutdown();
      Thread.sleep(2000);

      //starting the readNn
      readCluster = startReadCluster(false);
	  final NameNode readNn = readCluster.getNameNode();
      final NamenodeProtocols readNnProto = readNn.getRpcServer();
      FileSystem readFs = readCluster.getFileSystem();
      

      //try to read the file from the readNn (currently fails because datanodes are already dead) 
      FSDataInputStream in2 = readFs.open(fileTxt);
	  assertTrue(checkFile1(in2));
      
      //shutting down read cluster
      //readFs.close();

	  } finally {
		  if (writeCluster != null) {
			  writeCluster.shutdown();
		  }
		  if (readCluster != null) {
			  readCluster.shutdown();
		  }
	  }


  }

}
