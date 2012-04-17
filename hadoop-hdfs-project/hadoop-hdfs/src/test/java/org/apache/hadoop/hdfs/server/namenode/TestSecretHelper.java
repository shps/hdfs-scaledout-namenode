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
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManagerNN;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.mysql.clusterj.ClusterJException;

/**
 * 
 * @author wmalik
 *
 */
public class TestSecretHelper {
  public static final Log LOG = LogFactory.getLog(TestSecretHelper.class);
  private static final Configuration CONF = new HdfsConfiguration();
  
  @Before
  public void connect() throws IOException {
	  CONF.set(DFSConfigKeys.DFS_DB_DATABASE_KEY, "kthfs-getblocks");
	  DBConnector.setConfiguration(CONF);
	  DBAdmin.deleteAllTables(DBConnector.obtainSession(), "kthfs-getblocks");
  }

  @After
  public void disconnect() throws IOException {
  
  }

  
  @Test
  public void testCrudOperationsForKeys() throws ClusterJException, IOException {
	  BlockTokenSecretManagerNN tokenMgr = 
			  new BlockTokenSecretManagerNN(true, 60*60*1000L, 60*60*1000L); //60 minutes
	  
	  ExportedBlockKeys expKeys = tokenMgr.exportKeys();
	  assertEquals(2, expKeys.getAllKeys().length);
	  
  }
  
  
  
}
