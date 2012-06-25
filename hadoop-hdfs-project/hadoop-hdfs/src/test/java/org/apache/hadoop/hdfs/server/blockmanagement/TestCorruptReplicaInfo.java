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

import com.mysql.clusterj.ClusterJUserException;
import java.io.IOException;
import java.util.*;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/**
 * This test makes sure that 
 *   CorruptReplicasMap::numBlocksWithCorruptReplicas and
 *   CorruptReplicasMap::getCorruptReplicaBlockIds
 *   return the correct values
 */
public class TestCorruptReplicaInfo extends TestCase {

  private static final Log LOG =
                           LogFactory.getLog(TestCorruptReplicaInfo.class);
  private Map<Long, Block> block_map =
                           new HashMap<Long, Block>();

  // Allow easy block creation by block id
  // Return existing block if one with same block id already exists
  private Block getBlock(Long block_id) {
    if (!block_map.containsKey(block_id)) {
      block_map.put(block_id, new Block(block_id, 0, 0));
    }

    return block_map.get(block_id);
  }

  private Block getBlock(int block_id) {
    return getBlock((long) block_id);
  }

  public void testCorruptReplicaInfo() throws IOException,
                                              InterruptedException {

    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      EntityManager em = EntityManager.getInstance();
      DBConnector.beginTransaction();
      // Since we are persisting CorruptReplicasMap, we need to add begin and end transaction clause

      // Make sure initial values are returned correctly
      assertEquals("Number of corrupt blocks must initially be 0", 0, em.countAllCorruptedReplicas());
//      assertNull("Param n cannot be less than 0", crm.getCorruptReplicaBlockIds(-1, null));
//      assertNull("Param n cannot be greater than 100", crm.getCorruptReplicaBlockIds(101, null));
//      long[] l = crm.getCorruptReplicaBlockIds(0, null);
//      assertNotNull("n = 0 must return non-null", l);
//      assertEquals("n = 0 must return an empty list", 0, l.length);

      // create a list of block_ids. A list is used to allow easy validation of the
      // output of getCorruptReplicaBlockIds
      int NUM_BLOCK_IDS = 140;
      List<Long> block_ids = new LinkedList<Long>();
      for (int i = 0; i < NUM_BLOCK_IDS; i++) {
        block_ids.add((long) i);
      }

      List<DataNode> datanodes = cluster.getDataNodes();
      DatanodeDescriptor dn1 = new DatanodeDescriptor(datanodes.get(0).getDatanodeId());
      DatanodeDescriptor dn2 = new DatanodeDescriptor(datanodes.get(1).getDatanodeId());
      
      CorruptReplica corruptReplica = new CorruptReplica(getBlock(0).getBlockId(), dn1.getStorageID());
      em.persist(corruptReplica);
      assertEquals("Number of corrupt blocks not returning correctly", 1, em.countAllCorruptedReplicas());
      corruptReplica = new CorruptReplica(getBlock(1).getBlockId(), dn1.getStorageID());
      em.persist(corruptReplica);
      assertEquals("Number of corrupt blocks not returning correctly",
                   2, em.countAllCorruptedReplicas());
      
      corruptReplica = new CorruptReplica(getBlock(1).getBlockId(), dn2.getStorageID());
      em.persist(corruptReplica);
      assertEquals("Number of corrupt blocks not returning correctly", 3, em.countAllCorruptedReplicas());

      DBConnector.commit();
      DBConnector.beginTransaction();
      
      Collection<CorruptReplica> crs = em.findCorruptReplicaByBlockId(getBlock(1).getBlockId());
      for (CorruptReplica r : crs) {
        em.remove(r);
      }
      assertEquals("Number of corrupt blocks not returning correctly",
                   1, em.countAllCorruptedReplicas());

      crs = em.findCorruptReplicaByBlockId(getBlock(0).getBlockId());
      for (CorruptReplica r : crs) {
        em.remove(r);
      }
      assertEquals("Number of corrupt blocks not returning correctly",
                   0, em.countAllCorruptedReplicas());

      DBConnector.commit();
      DBConnector.beginTransaction();

      
      for (Long block_id : block_ids) {
        em.persist(new CorruptReplica(block_id, dn1.getStorageID()));
      }

      assertEquals("Number of corrupt blocks not returning correctly", NUM_BLOCK_IDS, em.countAllCorruptedReplicas());
//      DBConnector.commit();
//      DBConnector.beginTransaction();

//      assertTrue("First five block ids not returned correctly ",
//                 Arrays.equals(new long[]{0, 1, 2, 3, 4},
//                               crm.getCorruptReplicaBlockIds(5, null)));
//
//      LOG.info(crm.getCorruptReplicaBlockIds(10, 7L));
//      LOG.info(block_ids.subList(7, 18));
//
//      assertTrue("10 blocks after 7 not returned correctly ",
//                 Arrays.equals(new long[]{8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
//                               crm.getCorruptReplicaBlockIds(10, 7L)));

//      DBConnector.commit();
    } // end try
    catch (ClusterJUserException ex) {
      assertFalse("Exception in database operations. Exception: " + ex.getMessage(), true);
      DBConnector.safeRollback();
    }
    finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
