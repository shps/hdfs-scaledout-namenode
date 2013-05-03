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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode.NamenodeService;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.VolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeMultipleRegistrations {

  private static final Log LOG =
          LogFactory.getLog(TestDataNodeMultipleRegistrations.class);
  Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
  }

  /**
   * start multiple NNs and single DN and verifies per BP registrations and
   * handshakes.
   *
   * @throws IOException
   */
  @Test
  public void test2NNRegistration() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numNameNodes(2)
            .nameNodePort(9928).build();
    try {
      cluster.waitActive();
      NameNode nn1 = cluster.getNameNode(0);
      NameNode nn2 = cluster.getNameNode(1);
      assertNotNull("cannot create nn1", nn1);
      assertNotNull("cannot create nn2", nn2);

      StorageInfo storageInfo = FSImageTestUtil.getStorageInfo();
      String cid1 = storageInfo.getClusterID();
      int lv1 = storageInfo.getLayoutVersion();
      int ns1 = storageInfo.getNamespaceID();
      LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";uri="
              + nn1.getNameNodeAddress());

      // check number of volumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      Collection<VolumeInfo> volInfos = ((FSDataset) dn.data).getVolumeInfo();
      assertNotNull("No volumes in the fsdataset", volInfos);
      int i = 0;
      for (VolumeInfo vi : volInfos) {
        LOG.info("vol " + i++ + ";dir=" + vi.directory + ";fs= " + vi.freeSpace);
      }
      // number of volumes should be 2 - [data1, data2]
      assertEquals("number of volumes is wrong", 2, volInfos.size());

      for (NamenodeService bpos : dn.getAllBpOs()) {
        LOG.info("reg: bpid=" + "; name=" + bpos.bpRegistration.name + "; sid="
                + bpos.bpRegistration.storageID + "; nna=" + bpos.nnAddr);
      }

      NamenodeService bpos1 = dn.getAllBpOs()[0];
      NamenodeService bpos2 = dn.getAllBpOs()[1];

      // The order of bpos is not guaranteed, so fix the order
      if (bpos1.nnAddr.equals(nn2.getNameNodeAddress())) {
        NamenodeService tmp = bpos1;
        bpos1 = bpos2;
        bpos2 = tmp;
      }

      assertEquals("wrong nn address", bpos1.nnAddr,
              nn1.getNameNodeAddress());
      assertEquals("wrong nn address", bpos2.nnAddr,
              nn2.getNameNodeAddress());
      assertEquals("wrong cid", dn.getClusterId(), cid1);
      assertEquals("namespace should be same",
              bpos1.bpNSInfo.namespaceID, ns1);
      assertEquals("namespace should be same",
              bpos2.bpNSInfo.namespaceID, ns1);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * starts single nn and single dn and verifies registration and handshake
   *
   * @throws IOException
   */
  @Test
  public void testFedSingleNN() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .nameNodePort(9927).build();
    try {
      NameNode nn1 = cluster.getNameNode();
      assertNotNull("cannot create nn1", nn1);

      StorageInfo storageInfo = FSImageTestUtil.getStorageInfo();
      String cid1 = storageInfo.getClusterID();
      int lv1 = storageInfo.getLayoutVersion();
      LOG.info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";uri="
              + nn1.getNameNodeAddress());

      // check number of vlumes in fsdataset
      DataNode dn = cluster.getDataNodes().get(0);
      Collection<VolumeInfo> volInfos = ((FSDataset) dn.data).getVolumeInfo();
      assertNotNull("No volumes in the fsdataset", volInfos);
      int i = 0;
      for (VolumeInfo vi : volInfos) {
        LOG.info("vol " + i++ + ";dir=" + vi.directory + ";fs= " + vi.freeSpace);
      }
      // number of volumes should be 2 - [data1, data2]
      assertEquals("number of volumes is wrong", 2, volInfos.size());

      for (NamenodeService bpos : dn.getAllBpOs()) {
        LOG.info("reg: bpid=" + "; name=" + bpos.bpRegistration.name + "; sid="
                + bpos.bpRegistration.storageID + "; nna=" + bpos.nnAddr);
      }

      // try block report
      NamenodeService bpos1 = dn.getAllBpOs()[0];
      bpos1.lastBlockReport = 0;
      bpos1.blockReport();

      assertEquals("wrong nn address", bpos1.nnAddr,
              nn1.getNameNodeAddress());
      assertEquals("wrong cid", dn.getClusterId(), cid1);
      cluster.shutdown();

      // Ensure all the NamenodeService threads are shutdown
      assertEquals(0, dn.getAllBpOs().length);
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testClusterIdMismatch() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numNameNodes(2).
            nameNodePort(9928).build();
    try {
      cluster.waitActive();

      DataNode dn = cluster.getDataNodes().get(0);
      NamenodeService[] bposs = dn.getAllBpOs();
      LOG.info("dn bpos len (should be 2):" + bposs.length);
      Assert.assertEquals("should've registered with two namenodes", bposs.length, 2);

      // add another namenode
      cluster.addNameNode(conf, 9938);
      bposs = dn.getAllBpOs();
      LOG.info("dn bpos len (should be 3):" + bposs.length);
      Assert.assertEquals("should've registered with three namenodes", bposs.length, 3);

      // change cluster id and another Namenode
      StartupOption.FORMAT.setClusterId("DifferentCID");
      cluster.addNameNode(conf, 9948);
      NameNode nn4 = cluster.getNameNode(3);
      assertNotNull("cannot create nn4", nn4);

      bposs = dn.getAllBpOs();
      LOG.info("dn bpos len (still should be 3):" + bposs.length);
      Assert.assertEquals("should've registered with three namenodes", 3, bposs.length);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMiniDFSClusterWithMultipleNN() throws IOException {

    Configuration conf = new HdfsConfiguration();
    // start Federated cluster and add a node.
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numNameNodes(2).
            nameNodePort(9928).build();
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(1)Should be 2 namenodes", 2, cluster.getNumNameNodes());

    // add a node
    cluster.addNameNode(conf, 9929);
    Assert.assertEquals("(1)Should be 3 namenodes", 3, cluster.getNumNameNodes());
    cluster.shutdown();

    // 2. start with Federation flag set
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).federation(true).
            nameNodePort(9928).build();
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());

    // add a node
    cluster.addNameNode(conf, 9929);
    Assert.assertEquals("(2)Should be 2 namenodes", 2, cluster.getNumNameNodes());
    cluster.shutdown();

    // 3. start non-federated
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    Assert.assertNotNull(cluster);
    Assert.assertEquals("(2)Should be 1 namenodes", 1, cluster.getNumNameNodes());

    // add a node
    try {
      cluster.addNameNode(conf, 9929);
      Assert.fail("shouldn't be able to add another NN to non federated cluster");
    } catch (IOException e) {
      // correct 
      Assert.assertTrue(e.getMessage().startsWith("cannot add namenode"));
      Assert.assertEquals("(3)Should be 1 namenodes", 1, cluster.getNumNameNodes());
    } finally {
      cluster.shutdown();
    }
  }
}
