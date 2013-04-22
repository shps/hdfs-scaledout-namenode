
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.mysqlserver;

import com.mysql.clusterj.Session;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.BlockInfoClusterj.BlockInfoDTO;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ClusterjConnector;
import org.junit.Test;

/**
 *
 * @author hooman
 */
public class CountHelperTest {
  
  public CountHelperTest() {
  }

  /**
   * Test of countAllBlockInfo method, of class CountHelper.
   */
  @Test
  public void testCountAllBlockInfo() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_TYPE_KEY, "clusterj");
    StorageFactory.setConfiguration(new HdfsConfiguration());
    ClusterjConnector connector = (ClusterjConnector) StorageFactory.getConnector();
    connector.formatStorage();
    
    assert CountHelper.countAllBlockInfo() == 0; // block infos tables must be empty.
    
    Session session = connector.obtainSession();
    session.savePersistent(session.newInstance(BlockInfoDTO.class, 1L));
    session.savePersistent(session.newInstance(BlockInfoDTO.class, 2L));
    session.savePersistent(session.newInstance(BlockInfoDTO.class, 3L));
    session.flush();
    
    assert CountHelper.countAllBlockInfo() == 3; // There must be 3 rows.
    
  }
}
