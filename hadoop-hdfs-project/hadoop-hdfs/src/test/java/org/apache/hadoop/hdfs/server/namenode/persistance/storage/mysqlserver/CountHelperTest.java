package org.apache.hadoop.hdfs.server.namenode.persistance.storage.mysqlserver;

import com.mysql.clusterj.Session;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.CorruptReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ExcessReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.UnderReplicatedBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.BlockInfoClusterj.BlockInfoDTO;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ClusterjConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.CorruptReplicaClusterj.CorruptReplicaDTO;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ExcessReplicaClusterj.ExcessReplicaDTO;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.LeaseClusterj.LeaseDTO;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.UnderReplicatedBlockClusterj.UnderReplicatedBlocksDTO;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author hooman
 */
public class CountHelperTest {

  ClusterjConnector connector;

  public CountHelperTest() {
  }

  @Before
  public void init() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_TYPE_KEY, "clusterj");
    StorageFactory.setConfiguration(new HdfsConfiguration());
    connector = (ClusterjConnector) StorageFactory.getConnector();
    connector.formatStorage();
  }

  /**
   * Test of countAllBlockInfo method, of class CountHelper.
   */
  @Test
  public void testCountAllBlockInfo() throws Exception {
    assert CountHelper.countAll(BlockInfoDataAccess.TABLE_NAME) == 0; // the table must be empty.

    Session session = connector.obtainSession();
    session.savePersistent(session.newInstance(BlockInfoDTO.class, 1L));
    session.savePersistent(session.newInstance(BlockInfoDTO.class, 2L));
    session.savePersistent(session.newInstance(BlockInfoDTO.class, 3L));
    session.flush();

    assert CountHelper.countAll(BlockInfoDataAccess.TABLE_NAME) == 3; // There must be 3 rows.
  }

  /**
   * Test of countAllLeases method, of class CountHelper.
   */
  @Test
  public void testCountAllLeases() throws Exception {
    assert CountHelper.countAll(LeaseDataAccess.TABLE_NAME) == 0; // the table must be empty.

    Session session = connector.obtainSession();
    session.savePersistent(session.newInstance(LeaseDTO.class, "a"));
    session.savePersistent(session.newInstance(LeaseDTO.class, "b"));
    session.savePersistent(session.newInstance(LeaseDTO.class, "c"));
    session.flush();

    assert CountHelper.countAll(LeaseDataAccess.TABLE_NAME) == 3; // There must be 3 rows.
  }

  /**
   * Test of countAllCorruptReplicas method, of class CountHelper.
   */
  @Test
  public void testCountAllCorruptReplicas() throws Exception {
    assert CountHelper.countAll(CorruptReplicaDataAccess.TABLE_NAME) == 0; // the table must be empty.

    Session session = connector.obtainSession();
    session.savePersistent(session.newInstance(CorruptReplicaDTO.class, new Object[]{0L, "0"}));
    session.savePersistent(session.newInstance(CorruptReplicaDTO.class, new Object[]{1L, "1"}));
    session.savePersistent(session.newInstance(CorruptReplicaDTO.class, new Object[]{2L, "2"}));
    session.flush();

    assert CountHelper.countAll(CorruptReplicaDataAccess.TABLE_NAME) == 3; // There must be 3 rows.
  }

  /**
   * Test of countAllExcessReplicas method, of class CountHelper.
   */
  @Test
  public void testCountAllExcessReplicas() throws Exception {
    assert CountHelper.countAll(ExcessReplicaDataAccess.TABLE_NAME) == 0; // the table must be empty.

    Session session = connector.obtainSession();
    session.savePersistent(session.newInstance(ExcessReplicaDTO.class, new Object[]{0L, "0"}));
    session.savePersistent(session.newInstance(ExcessReplicaDTO.class, new Object[]{1L, "1"}));
    session.savePersistent(session.newInstance(ExcessReplicaDTO.class, new Object[]{2L, "2"}));
    session.flush();

    assert CountHelper.countAll(ExcessReplicaDataAccess.TABLE_NAME) == 3; // There must be 3 rows.
  }

  /**
   * Test of countAllUnderReplicatedBlocks method, of class CountHelper.
   */
  @Test
  public void testCountAllUnderReplicatedBlocks() throws Exception {
    assert CountHelper.countAll(UnderReplicatedBlockDataAccess.TABLE_NAME) == 0; // the table must be empty.

    Session session = connector.obtainSession();
    session.savePersistent(session.newInstance(UnderReplicatedBlocksDTO.class, 0L));
    session.savePersistent(session.newInstance(UnderReplicatedBlocksDTO.class, 1L));
    session.savePersistent(session.newInstance(UnderReplicatedBlocksDTO.class, 2L));
    session.flush();

    assert CountHelper.countAll(UnderReplicatedBlockDataAccess.TABLE_NAME) == 3; // There must be 3 rows.
  }

  /**
   * Test of countWithCriterion, of class CountHelper.
   */
  @Test
  public void testCountAllUnderReplicatedBlocksByLevel() throws Exception {
    assert CountHelper.countAll(UnderReplicatedBlockDataAccess.TABLE_NAME) == 0; // the table must be empty.

    Session session = connector.obtainSession();
    UnderReplicatedBlocksDTO ur1 = session.newInstance(UnderReplicatedBlocksDTO.class, 0L);
    ur1.setLevel(1);
    UnderReplicatedBlocksDTO ur2 = session.newInstance(UnderReplicatedBlocksDTO.class, 1L);
    ur2.setLevel(2);
    UnderReplicatedBlocksDTO ur3 = session.newInstance(UnderReplicatedBlocksDTO.class, 2L);
    ur3.setLevel(2);
    session.savePersistent(ur1);
    session.savePersistent(ur2);
    session.savePersistent(ur3);
    session.flush();

    assert CountHelper.countWithCriterion(UnderReplicatedBlockDataAccess.TABLE_NAME, "level=2") == 2; // There must be 3 rows.
    assert CountHelper.countWithCriterion(UnderReplicatedBlockDataAccess.TABLE_NAME, "level<3") == 3; // There must be 3 rows.
  }
}
