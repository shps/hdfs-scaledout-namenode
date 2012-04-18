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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.mysql.clusterj.ClusterJException;

/**
 * 
 * @author wmalik
 *
 */
public class TestLeaseHelper {
  public static final Log LOG = LogFactory.getLog(TestLeaseHelper.class);
  private static final Configuration CONF = new HdfsConfiguration();
  private static LeaseManager leaseManager = new LeaseManager(null);
  
  @Before
  public void connect() throws IOException {
	  DBConnector.setConfiguration(CONF);
	  LeaseHelper.initialize(leaseManager);
  }

  @After
  public void disconnect() throws IOException {
  
  }
  
  @Test
  public void testAddDeleteLease() throws ClusterJException {
          String holder = "wmalik";
	  int holderID = DFSUtil.getRandom().nextInt();
	  String src = "/home/wmalik/file"+holderID+".txt";
	  Lease lease = leaseManager.addLease(holder, src);
	  
          assertEquals("holder not persisted correctly", holder, lease.getHolder());
	  assertTrue("path not persisted correctly", lease.getPathsLocal().contains(src));
          
	  leaseManager.removeLease(lease, src, false);
	  
	  Lease leaseByPath = leaseManager.getLeaseByPath(src);
	  assertNull("lease not removed from database", leaseByPath);
  }
  
}
