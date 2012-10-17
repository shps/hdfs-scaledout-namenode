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

import java.io.EOFException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManagerNN;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestBlockTokenWithDFS {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 2 * BLOCK_SIZE;
  private static final String FILE_TO_READ = "/fileToRead.dat";
  private static final String FILE_TO_WRITE = "/fileToWrite.dat";
  private static final String FILE_TO_APPEND = "/fileToAppend.dat";
  private /*final*/ byte[] rawData = new byte[FILE_SIZE];

  {
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    Random r = new Random();
    r.nextBytes(rawData);
    
    // need to get atleast 2048 bytes via String data
//    StringBuilder data = new StringBuilder();
//    // completes 2028
//    for(int i=1; i<=78; i++) {
//      data.append("abcdefghijklmnopqrstuvwxyz");
//    }
//  // need 20 more bytes to fill it up
//  data.append("abcdefghijklmnopqrst");
//  System.out.println("total bytes: "+data.toString().getBytes().length);
//  rawData = data.toString().getBytes();
  
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
      System.out.println("**Reading file via FSDataInputStream...");
      // arguments of in.read(buffer, offset, length)  // reads up to 'length' bytes
      while ((nRead = in.read(toRead, totalRead, toRead.length - totalRead)) > 0) {
        totalRead += nRead;
      }
    }
    catch (IOException e) {
      System.out.println("**checkFile1 resulted in error. Exception: "+e.getMessage());
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
    	System.err.println("[thesis] W: fileToCheck.length ["+fileToCheck.length+"] != rawData.length ["+rawData.length+"]");
      return false;
    }
    for (int i = 0; i < fileToCheck.length; i++) {
      if (fileToCheck[i] != rawData[i]) {
        System.err.println("[thesis] W: fileToCheck["+i+"] != rawData["+i+"]");
        return false;
      }
    }
    return true;
  }

  // creates a file and returns a descriptor for writing to it
  private static FSDataOutputStream writeFile(FileSystem fileSys, Path name,
      short repl, long blockSize) throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf().getInt("io.file.buffer.size", 4096), repl, blockSize);
    return stm;
  }

  // try reading a block using a BlockReader directly
  private static void tryRead(Configuration conf, LocatedBlock lblock,
      boolean shouldSucceed) {
    InetSocketAddress targetAddr = null;
    Socket s = null;
    BlockReader blockReader = null;
    ExtendedBlock block = lblock.getBlock();
    try {
      DatanodeInfo[] nodes = lblock.getLocations();
      targetAddr = NetUtils.createSocketAddr(nodes[0].getName());
      s = new Socket();
      s.connect(targetAddr, HdfsServerConstants.READ_TIMEOUT);
      s.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);

      String file = BlockReaderFactory.getFileName(targetAddr, 
          "test-blockpoolid", block.getBlockId());
      blockReader = BlockReaderFactory.newBlockReader(s, file, block, 
          lblock.getBlockToken(), 0, -1, 
          conf.getInt("io.file.buffer.size", 4096));

    } catch (IOException ex) {
      if (ex instanceof InvalidBlockTokenException) {
        ex.printStackTrace();
        assertFalse("OP_READ_BLOCK: access token is invalid, "
            + "when it is expected to be valid", shouldSucceed);
        return;
      }
      fail("OP_READ_BLOCK failed due to reasons other than access token: "
          + StringUtils.stringifyException(ex));
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (IOException iex) {
        } finally {
          s = null;
        }
      }
    }
    if (blockReader == null) {
      fail("OP_READ_BLOCK failed due to reasons other than access token");
    }
    assertTrue("OP_READ_BLOCK: access token is valid, "
        + "when it is expected to be invalid", shouldSucceed);
  }

  // get a conf for testing
  private static Configuration getConf(int numDataNodes) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt("io.bytes.per.checksum", BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setBoolean("dfs.support.append", true);
    return conf;
  }

  /**
   * Gets all BlockTokenSecretManagerNN corresponding to all NNs in the system
   * Needed in order to apply configuration changes in tests
   * These configurations need to be applied in all namenodes for all BlockTokenSecretManagerNNs
   */
  public List<BlockTokenSecretManagerNN> getBlockTokenSecretManagers(MiniDFSCluster cluster) {
    List<BlockTokenSecretManagerNN> secretManagers = new ArrayList<BlockTokenSecretManagerNN>();
    for(int i=0; i<cluster.getNumNameNodes(); i++) {
      final NameNode nn = cluster.getNameNode(i);
      final BlockManager bm = nn.getNamesystem().getBlockManager();
      final BlockTokenSecretManagerNN sm = bm.getBlockTokenSecretManager();
      secretManagers.add(sm);
    }
    return secretManagers;
  }
  
  /**
   * testing that APPEND operation can handle token expiration when
   * re-establishing pipeline is needed
   */
  @Test
  public void testAppend() throws Exception {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    Configuration conf = getConf(numDataNodes);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());

      // [J] configuration changes - setting short token life time!
      // Needs to be applied in all namnodes
      List<BlockTokenSecretManagerNN> blockTokenSecretManagers = getBlockTokenSecretManagers(cluster);
      //final NameNode nn = cluster.getNameNode();
      //final BlockManager bm = nn.getNamesystem().getBlockManager();
      //final BlockTokenSecretManagerNN sm = bm.getBlockTokenSecretManager();

      // set a short token lifetime (1 second)
      SecurityTestUtil.setBlockTokenLifetime(blockTokenSecretManagers, 1000L);
      //SecurityTestUtil.setBlockTokenLifetime(sm, 1000L);
      Path fileToAppend = new Path(FILE_TO_APPEND);
      FileSystem fs = cluster.getFileSystem();

      // write a one-byte file
      FSDataOutputStream stm = writeFile(fs, fileToAppend,
          (short) numDataNodes, BLOCK_SIZE);
      stm.write(rawData, 0, 1);
      stm.close();
      // open the file again for append
      stm = fs.append(fileToAppend);
      int mid = rawData.length - 1;   // writing all bytes except for the last byte
      stm.write(rawData, 1, mid - 1);
      stm.hflush();

      /*
       * wait till token used in stm expires
       */
      Token<BlockTokenIdentifier> token = DFSTestUtil.getBlockToken(stm);
      while (!SecurityTestUtil.isBlockTokenExpired(token)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
        } 
      }

      // remove a datanode to force re-establishing pipeline
      System.out.println("**Stopping datanode ["+cluster.getDataNodes().get(0).getMachineName() +"]");
      cluster.stopDataNode(0);
      // append the rest of the file
      System.out.println("**remaining bytes to write ["+(rawData.length-mid)+"], current.pos: "+mid);
      stm.write(rawData, mid, rawData.length - mid);
      stm.close();
      // check if   append is successful
        System.out.println("**Verifying file by reading");
      FSDataInputStream in5 = fs.open(fileToAppend);
      assertTrue(checkFile1(in5));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * testing that WRITE operation can handle token expiration when
   * re-establishing pipeline is needed
   * 
   * Thhis test case is designed for Single-Namenode architecture
   */
  @Test
  public void testWrite() throws Exception {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    Configuration conf = getConf(numDataNodes);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());

      // [J] configuration changes - setting short token life time!
      // Needs to be applied in all namnodes
      List<BlockTokenSecretManagerNN> blockTokenSecretManagers = getBlockTokenSecretManagers(cluster);
      //final NameNode nn = cluster.getNameNode();
      //final BlockManager bm = nn.getNamesystem().getBlockManager();
      //final BlockTokenSecretManagerNN sm = bm.getBlockTokenSecretManager();

      // set a short token lifetime (1 second)
      SecurityTestUtil.setBlockTokenLifetime(blockTokenSecretManagers, 1000L);
      //SecurityTestUtil.setBlockTokenLifetime(sm, 1000L);
      Path fileToWrite = new Path(FILE_TO_WRITE);
      FileSystem fs = cluster.getFileSystem();

      FSDataOutputStream stm = writeFile(fs, fileToWrite, (short) numDataNodes,
          BLOCK_SIZE);
      // write a partial block
      int mid = rawData.length - 1;
      stm.write(rawData, 0, mid);
      stm.hflush();

      /*
       * wait till token used in stm expires
       */
      Token<BlockTokenIdentifier> token = DFSTestUtil.getBlockToken(stm);
      while (!SecurityTestUtil.isBlockTokenExpired(token)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }
      }

      // remove a datanode to force re-establishing pipeline
      cluster.stopDataNode(0);
      // write the rest of the file
      stm.write(rawData, mid, rawData.length - mid);
      stm.close();
      // check if write is successful
      FSDataInputStream in4 = fs.open(fileToWrite);
      assertTrue(checkFile1(in4));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRead() throws Exception {
    MiniDFSCluster cluster = null;
    int numDataNodes = 2;
    Configuration conf = getConf(numDataNodes);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());

      // [J] configuration changes - setting short token life time!
      // Needs to be applied in all namnodes
      List<BlockTokenSecretManagerNN> blockTokenSecretManagers = getBlockTokenSecretManagers(cluster);
      //final NameNode nn = cluster.getNameNode();
      //final NamenodeProtocols nnProto = nn.getRpcServer();
      //final BlockManager bm = nn.getNamesystem().getBlockManager();
      //final BlockTokenSecretManagerNN sm = bm.getBlockTokenSecretManager();

      // set a short token lifetime (1 second)
      SecurityTestUtil.setBlockTokenLifetime(blockTokenSecretManagers, 1000L);
      //SecurityTestUtil.setBlockTokenLifetime(sm, 1000L);
      final NamenodeProtocols nnProto = cluster.getNameNode().getRpcServer();

      Path fileToRead = new Path(FILE_TO_READ);
      FileSystem fs = cluster.getFileSystem();
      createFile(fs, fileToRead);

      /*
       * setup for testing expiration handling of cached tokens
       */

      // read using blockSeekTo(). Acquired tokens are cached in in1
      System.out.println("**Reading file[1] '"+fileToRead+"'");
      FSDataInputStream in1 = fs.open(fileToRead);
      assertTrue(checkFile1(in1));
      // read using blockSeekTo(). Acquired tokens are cached in in2
      System.out.println("**Reading file[2] '"+fileToRead+"'");
      FSDataInputStream in2 = fs.open(fileToRead);
      assertTrue(checkFile1(in2));
      // read using fetchBlockByteRange(). Acquired tokens are cached in in3
      FSDataInputStream in3 = fs.open(fileToRead);
      assertTrue(checkFile2(in3));

      /*
       * testing READ interface on DN using a BlockReader
       */

      new DFSClient(new InetSocketAddress("localhost",cluster.getNameNodePort()), conf);
      List<LocatedBlock> locatedBlocks = nnProto.getBlockLocations(
          FILE_TO_READ, 0, FILE_SIZE).getLocatedBlocks();
      LocatedBlock lblock = locatedBlocks.get(0); // first block
      Token<BlockTokenIdentifier> myToken = lblock.getBlockToken();
      // verify token is not expired
      assertFalse(SecurityTestUtil.isBlockTokenExpired(myToken));
      // read with valid token, should succeed
      tryRead(conf, lblock, true);

      /*
       * wait till myToken and all cached tokens in in1, in2 and in3 expire
       */

      while (!SecurityTestUtil.isBlockTokenExpired(myToken)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }
      }

      /*
       * continue testing READ interface on DN using a BlockReader
       */

      // verify token is expired
      assertTrue(SecurityTestUtil.isBlockTokenExpired(myToken));
      // read should fail
      tryRead(conf, lblock, false);
      // use a valid new token
      // [J] lblock.setBlockToken(sm.generateToken(lblock.getBlock(), EnumSet.of(BlockTokenSecretManager.AccessMode.READ)));
      lblock.setBlockToken(blockTokenSecretManagers.get(0).generateToken(lblock.getBlock(), EnumSet.of(BlockTokenSecretManager.AccessMode.READ)));
      // read should succeed
      tryRead(conf, lblock, true);
      // use a token with wrong blockID
      ExtendedBlock wrongBlock = new ExtendedBlock(lblock.getBlock()
          .getBlockPoolId(), lblock.getBlock().getBlockId() + 1);
      //lblock.setBlockToken(sm.generateToken(wrongBlock, EnumSet.of(BlockTokenSecretManager.AccessMode.READ)));
      lblock.setBlockToken(blockTokenSecretManagers.get(0).generateToken(wrongBlock, EnumSet.of(BlockTokenSecretManager.AccessMode.READ)));
      // read should fail
      tryRead(conf, lblock, false);
      // use a token with wrong access modes
      //lblock.setBlockToken(sm.generateToken(lblock.getBlock(), EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE, BlockTokenSecretManager.AccessMode.COPY, BlockTokenSecretManager.AccessMode.REPLACE)));
      lblock.setBlockToken(blockTokenSecretManagers.get(0).generateToken(lblock.getBlock(), 
                                                                         EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE, 
                                                                                    BlockTokenSecretManager.AccessMode.COPY, 
                                                                                    BlockTokenSecretManager.AccessMode.REPLACE)));
      // read should fail
      tryRead(conf, lblock, false);

      // set a long token lifetime for future tokens
      SecurityTestUtil.setBlockTokenLifetime(blockTokenSecretManagers, 600 * 1000L);

      /*
       * testing that when cached tokens are expired, DFSClient will re-fetch
       * tokens transparently for READ.
       */

      // confirm all tokens cached in in1 are expired by now
      List<LocatedBlock> lblocks = DFSTestUtil.getAllBlocks(in1);
      for (LocatedBlock blk : lblocks) {
        assertTrue(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() is able to re-fetch token transparently
      in1.seek(0);
      assertTrue(checkFile1(in1));

      // confirm all tokens cached in in2 are expired by now
      List<LocatedBlock> lblocks2 = DFSTestUtil.getAllBlocks(in2);
      for (LocatedBlock blk : lblocks2) {
        assertTrue(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() is able to re-fetch token transparently (testing
      // via another interface method)
      assertTrue(in2.seekToNewSource(0));
      assertTrue(checkFile1(in2));

      // confirm all tokens cached in in3 are expired by now
      List<LocatedBlock> lblocks3 = DFSTestUtil.getAllBlocks(in3);
      for (LocatedBlock blk : lblocks3) {
        assertTrue(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify fetchBlockByteRange() is able to re-fetch token transparently
      assertTrue(checkFile2(in3));

      /*
       * testing that after datanodes are restarted on the same ports, cached
       * tokens should still work and there is no need to fetch new tokens from
       * namenode. This test should run while namenode is down (to make sure no
       * new tokens can be fetched from namenode).
       */

      // restart datanodes on the same ports that they currently use
      assertTrue(cluster.restartDataNodes(true));
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());
      //cluster.shutdownNameNode(0);
      cluster.shutdownNameNodes();
      
      // confirm tokens cached in in1 are still valid
      lblocks = DFSTestUtil.getAllBlocks(in1);
      for (LocatedBlock blk : lblocks) {
        assertFalse(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() still works (forced to use cached tokens)
      in1.seek(0);
      assertTrue(checkFile1(in1));

      // confirm tokens cached in in2 are still valid
      lblocks2 = DFSTestUtil.getAllBlocks(in2);
      for (LocatedBlock blk : lblocks2) {
        assertFalse(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify blockSeekTo() still works (forced to use cached tokens)
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));

      // confirm tokens cached in in3 are still valid
      lblocks3 = DFSTestUtil.getAllBlocks(in3);
      for (LocatedBlock blk : lblocks3) {
        assertFalse(SecurityTestUtil.isBlockTokenExpired(blk.getBlockToken()));
      }
      // verify fetchBlockByteRange() still works (forced to use cached tokens)
      assertTrue(checkFile2(in3));

      /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
       * testing that when namenode is restarted, cached tokens should still
       * work and there is no need to fetch new tokens from namenode. Like the
       * previous test, this test should also run while namenode is down. The
       * setup for this test depends on the previous test.
       * 
       * The DFSInputStream holds the cached tokens before the NN went down. 
       * These tokens were also exported to the DNs so it should have it with them as well
       * So the client can communicate with these cached tokens to read blocks from DNs
       */

      // restart the namenode and then shut it down for test
      //cluster.restartNameNode(0);
      //cluster.shutdownNameNode(0);
      System.out.println("**About to restart all namenodes. DNs register and get new keys and cache them...");
      cluster.restartAllNameNodes(false);
      cluster.waitActive();
      cluster.waitClusterUp();
      System.out.println("**About to shutdown all namenodes. DNs should still have keys...");
      cluster.shutdownNameNodes();

      System.out.println("**All namenodes have been shutdown with keys...");
      // verify blockSeekTo() still works (forced to use cached tokens)
      in1.seek(0);
      assertTrue(checkFile1(in1));
      // verify again blockSeekTo() still works (forced to use cached tokens)
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));
      // verify fetchBlockByteRange() still works (forced to use cached tokens)
      assertTrue(checkFile2(in3));

      /*
       * testing that after both namenode and datanodes got restarted (namenode
       * first, followed by datanodes), DFSClient can't access DN without
       * re-fetching tokens and is able to re-fetch tokens transparently. The
       * setup of this test depends on the previous test.
       * 
       * [J] Incase of multiple namenode architecture, for this test case, we need to shut down all namenodes
       */

      // restore the cluster and restart the datanodes for test
      //cluster.restartNameNode(0);
      cluster.restartAllNameNodes(false);
      cluster.waitActive();
      cluster.isClusterUp();
      assertTrue(cluster.restartDataNodes(true));
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());

      // shutdown namenode so that DFSClient can't get new tokens from namenode
      //cluster.shutdownNameNode(0);
      cluster.shutdownNameNodes();

      // verify blockSeekTo() fails (cached tokens become invalid)
      in1.seek(0);
      assertFalse(checkFile1(in1));
      // verify fetchBlockByteRange() fails (cached tokens become invalid)
      assertFalse(checkFile2(in3));

      // restart the namenode to allow DFSClient to re-fetch tokens
      //cluster.restartNameNode(0);
      cluster.restartAllNameNodes(false);
      cluster.waitActive();
      
      // verify blockSeekTo() works again (by transparently re-fetching
      // tokens from namenode)
      in1.seek(0);
      assertTrue(checkFile1(in1));
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));
      // verify fetchBlockByteRange() works again (by transparently
      // re-fetching tokens from namenode)
      assertTrue(checkFile2(in3));

      /*
       * testing that when datanodes are restarted on different ports, DFSClient
       * is able to re-fetch tokens transparently to connect to them
       */

      // restart datanodes on newly assigned ports
      assertTrue(cluster.restartDataNodes(true)); //FIXME: W
      cluster.waitActive();
      assertEquals(numDataNodes, cluster.getDataNodes().size());
      // verify blockSeekTo() is able to re-fetch token transparently
      in1.seek(0);
      assertTrue(checkFile1(in1));
      // verify blockSeekTo() is able to re-fetch token transparently
      in2.seekToNewSource(0);
      assertTrue(checkFile1(in2));
      // verify fetchBlockByteRange() is able to re-fetch token transparently
      assertTrue(checkFile2(in3));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Integration testing of access token, involving NN, DN, and Balancer
   */
  //@Test
  public void testEnd2End() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    new TestBalancer().integrationTest(conf);
  }
}
