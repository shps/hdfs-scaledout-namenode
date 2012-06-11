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

import java.util.ArrayList;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/**
 * This class tests that methods in DatanodeDescriptor
 */
public class TestDatanodeDescriptor extends TestCase {
  /**
   * Test that getInvalidateBlocks observes the maxlimit.
   */
  public void testGetInvalidateBlocks() throws Exception {
    final int MAX_BLOCKS = 10;
    final int REMAINING_BLOCKS = 2;
    final int MAX_LIMIT = MAX_BLOCKS - REMAINING_BLOCKS;
    
    DatanodeDescriptor dd = new DatanodeDescriptor();
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    for (int i=0; i<MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP));
    }
    dd.addBlocksToBeInvalidated(blockList);
    Block[] bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, MAX_LIMIT);
    bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, REMAINING_BLOCKS);
  }
  
  public void testBlocksCounter() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_DB_DATABASE_KEY, DFSConfigKeys.DFS_DB_DATABASE_DEFAULT);
    DBConnector.setConfiguration(conf);
    EntityManager em = EntityManager.getInstance();
    
    DatanodeDescriptor dd = new DatanodeDescriptor();
    assertEquals(0, dd.numBlocks());
    DBConnector.beginTransaction();
    
    BlockInfo blk = new BlockInfo(new Block(1L));
    BlockInfo blk1 = new BlockInfo(new Block(2L));
    em.persist(blk);
    em.persist(blk1);
    // add first block
    IndexedReplica r1 = blk.addReplica(dd);
    assertNotNull(r1);
    em.persist(r1);
    
    assertEquals(1, dd.numBlocks());
    // remove a non-existent block
    IndexedReplica removeReplica = blk1.removeReplica(dd);
    assertNull(removeReplica);
    assertEquals(1, dd.numBlocks());
    // add an existent block
    r1 = blk.addReplica(dd);
    assertNull(r1);
    assertEquals(1, dd.numBlocks());
    // add second block
    r1 = blk1.addReplica(dd);
    assertNotNull(r1);
    em.persist(r1);
    assertEquals(2, dd.numBlocks());
    // remove first block
    removeReplica = blk.removeReplica(dd);
    assertNotNull(removeReplica);
    em.remove(removeReplica);
    assertEquals(1, dd.numBlocks());
    // remove second block
    removeReplica = blk1.removeReplica(dd);
    assertNotNull(removeReplica);
    em.remove(removeReplica);
    assertEquals(0, dd.numBlocks()); 
    //We rollbalck here because this generated data does not have the required 
    //integrity to be persisted in database, for instance new block should be 
    //blong to an inode.
    DBConnector.safeRollback();
  }
}
