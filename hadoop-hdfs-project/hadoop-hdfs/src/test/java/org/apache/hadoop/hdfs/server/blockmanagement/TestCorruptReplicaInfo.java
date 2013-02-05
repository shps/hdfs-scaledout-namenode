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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;

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
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      final List<Long> block_ids = new LinkedList<Long>();
      final List<DataNode> datanodes = cluster.getDataNodes();
      final DatanodeDescriptor dn1 = new DatanodeDescriptor(datanodes.get(0).getDatanodeId());
      final DatanodeDescriptor dn2 = new DatanodeDescriptor(datanodes.get(1).getDatanodeId());
      final int NUM_BLOCK_IDS = 140;

      new TransactionalRequestHandler(OperationType.TEST_CORRUPT_REPLICA_INFO) {

        @Override
        public Object performTask() throws PersistanceException, IOException {
          // Since we are persisting CorruptReplicasMap, we need to add begin and end transaction clause

          // Make sure initial values are returned correctly
          assertEquals("Number of corrupt blocks must initially be 0", 0, EntityManager.count(CorruptReplica.Counter.All));
//      assertNull("Param n cannot be less than 0", crm.getCorruptReplicaBlockIds(-1, null));
//      assertNull("Param n cannot be greater than 100", crm.getCorruptReplicaBlockIds(101, null));
//      long[] l = crm.getCorruptReplicaBlockIds(0, null);
//      assertNotNull("n = 0 must return non-null", l);
//      assertEquals("n = 0 must return an empty list", 0, l.length);

          // create a list of block_ids. A list is used to allow easy validation of the
          // output of getCorruptReplicaBlockIds
          for (int i = 0; i < NUM_BLOCK_IDS; i++) {
            block_ids.add((long) i);
          }


          CorruptReplica corruptReplica = new CorruptReplica(getBlock(0).getBlockId(), dn1.getStorageID());
          EntityManager.add(corruptReplica);
          assertEquals("Number of corrupt blocks not returning correctly", 1, EntityManager.count(CorruptReplica.Counter.All));
          corruptReplica = new CorruptReplica(getBlock(1).getBlockId(), dn1.getStorageID());
          EntityManager.add(corruptReplica);
          assertEquals("Number of corrupt blocks not returning correctly",
                  2, EntityManager.count(CorruptReplica.Counter.All));

          corruptReplica = new CorruptReplica(getBlock(1).getBlockId(), dn2.getStorageID());
          EntityManager.add(corruptReplica);
          assertEquals("Number of corrupt blocks not returning correctly", 3, EntityManager.count(CorruptReplica.Counter.All));
          return null;
        }

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          // TODO no lock needed
        }
      }.handle();

      new TransactionalRequestHandler(OperationType.TEST_CORRUPT_REPLICA_INFO2) {

        @Override
        public Object performTask() throws PersistanceException, IOException {

          Collection<CorruptReplica> crs = EntityManager.findList(CorruptReplica.Finder.ByBlockId, getBlock(1).getBlockId());
          for (CorruptReplica r : crs) {
            EntityManager.remove(r);
          }
          assertEquals("Number of corrupt blocks not returning correctly",
                  1, EntityManager.count(CorruptReplica.Counter.All));

          crs = EntityManager.findList(CorruptReplica.Finder.ByBlockId, getBlock(0).getBlockId());
          for (CorruptReplica r : crs) {
            EntityManager.remove(r);
          }
          assertEquals("Number of corrupt blocks not returning correctly",
                  0, EntityManager.count(CorruptReplica.Counter.All));
          return null;
        }

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          throw new UnsupportedOperationException("Not supported yet.");
        }
      }.handle();

      new TransactionalRequestHandler(OperationType.TEST_CORRUPT_REPLICA_INFO3) {

        @Override
        public Object performTask() throws PersistanceException, IOException {


          for (Long block_id : block_ids) {
            EntityManager.add(new CorruptReplica(block_id, dn1.getStorageID()));
          }

          assertEquals("Number of corrupt blocks not returning correctly", NUM_BLOCK_IDS, EntityManager.count(CorruptReplica.Counter.All));

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
          return null;
        }

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          throw new UnsupportedOperationException("Not supported yet.");
        }
      }.handle();

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
