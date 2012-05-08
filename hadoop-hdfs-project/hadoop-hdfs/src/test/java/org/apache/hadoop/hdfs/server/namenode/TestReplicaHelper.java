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



import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManagerNN;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.mysql.clusterj.ClusterJException;

/**
 * Tests the ReplicaHelper class
 * @author Wasif Riaz Malik 
 *
 */
public class TestReplicaHelper {
  public static final Log LOG = LogFactory.getLog(TestReplicaHelper.class);
  private static final Configuration CONF = new HdfsConfiguration();
  
  @Before
  public void connect() throws IOException {
	  CONF.set(DFSConfigKeys.DFS_DB_DATABASE_KEY, DFSConfigKeys.DFS_DB_DATABASE_DEFAULT);
	  DBConnector.setConfiguration(CONF);
	  DBConnector.formatDB();
  }

  @After
  public void disconnect() throws IOException {
  
  }

  
  @Test
  public void testCrudOperations() throws IOException {
    DatanodeID nodeID = new DatanodeID("wasif:31337", "what-a-cool-storage-id", 11111, 22222);
    DatanodeID nodeID2 = new DatanodeID("wasif:31338", "another-cool-storage-id-im-on-a-roll", 33333, 44444);
    DatanodeDescriptor targets[] = new DatanodeDescriptor[2];
    targets[0] = new DatanodeDescriptor(nodeID);
    targets[1] = new DatanodeDescriptor(nodeID2);
    
    
    //create a block info under construction with expected locations
    Block block = new Block(123, 32, 1001);
    BlockInfoUnderConstruction bInfoUc = new BlockInfoUnderConstruction(block, 1);
    bInfoUc.setBlockUCState(BlockUCState.UNDER_CONSTRUCTION);
    bInfoUc.setExpectedLocations(targets, false);
    BlocksHelper.putBlockInfo(bInfoUc, false);
    
    //fetch the values and verify
    DatanodeDescriptor[] actualTargets = bInfoUc.getExpectedLocations();
    assertEquals("wasif:31337", actualTargets[0].getName());
    assertEquals("wasif:31338", actualTargets[1].getName());
    assertEquals(2, bInfoUc.getNumExpectedLocations());
	  
  }
  
  
  
}
