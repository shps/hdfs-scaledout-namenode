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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

/**
 * This class tests the internals of PendingReplicationBlocks.java
 */
public class TestPendingReplication extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestPendingReplication.class);
  final static int TIMEOUT = 3;     // 3 seconds

  public void testPendingReplication() {
    try {
      final PendingReplicationBlocks pendingReplications = new PendingReplicationBlocks(TIMEOUT * 1000);
      StorageFactory.setConfiguration(new HdfsConfiguration());
      StorageFactory.getConnector().formatStorage();
      try {
        new TransactionalRequestHandler(OperationType.TEST_PENDING_REPLICATION) {

          @Override
          public Object performTask() throws PersistanceException, IOException {
            //
            // Add 10 blocks to pendingReplications.
            //
            for (int i = 0; i < 10; i++) {
              Block block = new Block(i, i, 0);
              pendingReplications.add(block, i);
            }

            assertEquals("Size of pendingReplications ",
                    10, pendingReplications.size());
            return null;
          }
        }.handle();

        final Block blk = new Block(8, 8, 0);

        new TransactionalRequestHandler(OperationType.TEST_PENDING_REPLICATION2) {

          @Override
          public Object performTask() throws PersistanceException, IOException {
            //
            // remove one item and reinsert it
            //
            pendingReplications.remove(blk);             // removes one replica
            assertEquals("pendingReplications.getNumReplicas ",
                    7, pendingReplications.getNumReplicas(blk));

            for (int i = 0; i < 7; i++) {
              pendingReplications.remove(blk);           // removes all replicas
            }

            assertTrue(pendingReplications.size() == 9);
            return null;
          }
        }.handle();

        new TransactionalRequestHandler(OperationType.TEST_PENDING_REPLICATION3) {

          @Override
          public Object performTask() throws PersistanceException, IOException {
            pendingReplications.add(blk, 8);
            assertTrue(pendingReplications.size() == 10);

            //
            // verify that the number of replicas returned
            // are sane.
            //
            for (int i = 0; i < 10; i++) {
              Block block = new Block(i, i, 0);
              int numReplicas = pendingReplications.getNumReplicas(block);
              assertTrue(numReplicas == i);
            }
            return null;
          }
        }.handle();

        //
        // verify that nothing has timed out so far
        //
        assertTrue(pendingReplications.getTimedOutBlocks(OperationType.TEST_PENDING_REPLICATION4) == null);

        //
        // Wait for one second and then insert some more items.
        //
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }

        new TransactionalRequestHandler(OperationType.TEST_PENDING_REPLICATION4) {

          @Override
          public Object performTask() throws PersistanceException, IOException {
            for (int i = 10; i < 15; i++) {
              Block block = new Block(i, i, 0);
              pendingReplications.add(block, i);
            }
            assertTrue(pendingReplications.size() == 15);

            //
            // Wait for everything to timeout.
            //
            int loop = 0;
            while (pendingReplications.size() > 0) {
              try {
                Thread.sleep(1000);
              } catch (Exception e) {
              }
              loop++;
            }
            LOG.info("Had to wait for " + loop
                    + " seconds for the lot to timeout");
            //
            // Verify that everything has timed out.
            //
            assertEquals("Size of pendingReplications ",
                    0, pendingReplications.size());
            return null;
          }
        }.handle();
        List<PendingBlockInfo> timedOut = pendingReplications.getTimedOutBlocks(OperationType.TEST_PENDING_REPLICATION4);
        assertTrue(timedOut != null && timedOut.size() == 15);
        for (int i = 0; i < timedOut.size(); i++) {
          assertTrue(timedOut.get(i).getBlockId() < 15);
        }
      } catch (IOException ex) {
        Logger.getLogger(TestPendingReplication.class.getName()).log(Level.SEVERE, null, ex);
      }

    } catch (StorageException ex) {
      Logger.getLogger(TestPendingReplication.class.getName()).log(Level.SEVERE, null, ex);
    }

  }
}
