package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.junit.Test;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class DerbyConnectorTest {

  DerbyConnector connector = DerbyConnector.INSTANCE;

  @Test
  public void testInMemoryDB() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_TYPE_KEY, DerbyConnector.DERBY_EMBEDDED);
    conf.set(DFSConfigKeys.DFS_STORAGE_DERBY_PROTOCOL_KEY, DerbyConnector.DEFAULT_DERBY_EMBEDDED_PROTOCOL);
    connector.setConfiguration(conf);
    try {
      connector.formatStorage();
      //seting up the DB again should not cause any problem
      connector.setConfiguration(conf);
      connector.stopStorage();
      //setting up the DB after stopping should not cause any problem
      connector.setConfiguration(conf);
      connector.stopStorage();
    } catch (StorageException ex) {
      assert false : ex.getMessage();
    }
  }

  /**
   * This test requires a Derby server be listening on the default protocol.
   */
  @Test
  public void testNetworkServer() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_TYPE_KEY, DerbyConnector.DERBY_NETWORK_SERVER);
    conf.set(DFSConfigKeys.DFS_STORAGE_DERBY_PROTOCOL_KEY, DerbyConnector.DEFAULT_DERBY_NETWORK_SERVER_PROTOCOL);
    try {
      connector.setConfiguration(conf);
      connector.formatStorage();
      connector.stopStorage();
      //seting up the DB again should not cause any problem
      connector.setConfiguration(conf);
      connector.formatStorage();
      // Setting up again should not cause any problem
      connector.setConfiguration(conf);
      connector.stopStorage();
    } catch (StorageException ex) {
      assert false : ex.getMessage();
    }
  }
}
