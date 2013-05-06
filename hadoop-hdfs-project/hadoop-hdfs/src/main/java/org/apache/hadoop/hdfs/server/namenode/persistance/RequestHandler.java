
package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class RequestHandler {
  
  public enum OperationType {
    // NameNodeRpcServer

    INITIALIZE, ACTIVATE, META_SAVE, SET_PERMISSION, SET_OWNER,
    GET_BLOCK_LOCATIONS, GET_STATS, CONCAT, SET_TIMES, CREATE_SYM_LINK, GET_PREFERRED_BLOCK_SIZE,
    SET_REPLICATION, START_FILE, RECOVER_LEASE, APPEND_FILE, GET_ADDITIONAL_BLOCK,
    GET_ADDITIONAL_DATANODE, ABANDON_BLOCK, COMPLETE_FILE, RENAME_TO, RENAME_TO2,
    DELETE, GET_FILE_INFO, MKDIRS, GET_CONTENT_SUMMARY, SET_QUOTA,
    FSYNC, COMMIT_BLOCK_SYNCHRONIZATION, RENEW_LEASE, GET_LISTING, REGISTER_DATANODE,
    HANDLE_HEARTBEAT, GET_MISSING_BLOCKS_COUNT, SAVE_NAMESPACE, SAFE_MODE_MONITOR,
    SET_SAFE_MODE, GET_BLOCKS_TOTAL, PROCESS_DISTRIBUTED_UPGRADE, GET_FS_STATE,
    UPDATE_BLOCK_FOR_PIPELINE, UPDATE_PIPELINE, LIST_CORRUPT_FILE_BLOCKS,
    GET_DELEGATION_TOKEN, RENEW_DELEGATION_TOKEN, CANCEL_DELEGATION_TOKEN,
    GET_SAFE_MODE, GET_NUMBER_OF_MISSING_BLOCKS, GET_PENDING_DELETION_BLOCKS_COUNT, GET_EXCESS_BLOCKS_COUNT,
    GET_ROOT,
    //BlockManager
    FIND_AND_MARK_BLOCKS_AS_CORRUPT, PREPARE_PROCESS_REPORT, PROCESS_FIRST_BLOCK_REPORT, PROCESS_REPORT, AFTER_PROCESS_REPORT,
    BLOCK_RECEIVED_AND_DELETED,
    REPLICATION_MONITOR,
    // DatanodeManager
    REMOVE_DATANODE, REFRESH_NODES,
    //DecommisionManager
    DECOMMISION_MONITOR,
    //HeartbeatManager
    HEARTBEAT_MONITOR,
    //LeaseManager
    PREPARE_LEASE_MANAGER_MONITOR, LEASE_MANAGER_MONITOR,
    // LeaderElection
    SELECT_ALL_NAMENODES, LEADER_EXIT, GET_ALL_NAME_NODES, GET_LEADER, LEADER_ELECTION, UPDATE_LEADER_COUNTER, REMOVE_PREV_LEADERS,
    // BlockTokenSecretManagerNN
    ADD_BLOCK_TOKENS, GET_ALL_BLOCK_TOKENS, GET_BLOCK_TOKENS, REMOVE_ALL, GET_KEY_BY_TYPE,REMOVE_BLOCK_KEY, UPDATE_BLOCK_KEYS, GET_KEY_BY_ID,
    // Block Generationstamp
    GET_GENERATION_STAMP, SET_GENERATION_STAMP,
    //FSNamesystem
    TOTAL_FILES,GET_STORAGE_INFO,
    //ClusterInfos
    GET_CLUSTER_INFO,
    // NNStorage
    ADD_STORAGE_INFO,
    //NamenodeJspHElper
    GET_SAFE_MODE_TEXT,GENERATE_HEALTH_REPORT, GET_INODE, TO_XML_BLOCK_INFO,
    TO_XML_CORRUPT_BLOCK_INFO,
    // TestNamenodePing
    COUNT_LEASE,
    // BLockManagerTestUtil
    UPDATE_STATE, GET_REPLICA_INFO, GET_NUMBER_OF_RACKS, GET_COMPUTED_DATANODE_WORK,
    // TestBlockManager
    REMOVE_NODE, FULFILL_PIPELINE, BLOCK_ON_NODES, SCHEDULE_SINGLE_REPLICATION,
    // TestComputeInvalidatedWork
    COMP_INVALIDATE,
    // TestCorruptReplicaInfo
    TEST_CORRUPT_REPLICA_INFO, TEST_CORRUPT_REPLICA_INFO2, TEST_CORRUPT_REPLICA_INFO3,
    // TestDatanodeDescriptor
    TEST_BLOCKS_COUNTER,
    // TestNodeCount
    COUNT_NODES,
    // TestOverReplicatedBlocks
    TEST_PROCESS_OVER_REPLICATED_BLOCKS,
    // TestPendingReplication
    TEST_PENDING_REPLICATION, TEST_PENDING_REPLICATION2, TEST_PENDING_REPLICATION3,
    TEST_PENDING_REPLICATION4,
    // TestUnderReplicatedBlocks
    SET_REPLICA_INCREAMENT,
    // TestBlockReport
    TEST_BLOCK_REPORT, TEST_BLOCK_REPORT2, PRINT_STAT,
    // TestBlockUnderConstruction
    VERIFY_FILE_BLOCKS,
    // TestFsLimits
    VERIFY_FS_LIMITS,
    // TestSafeMode
    TEST_DATANODE_THRESHOLD,
    // TestDfsRename
    COUNT_LEASE_DFS_RENAME,
    // NameNodeAdapter
    GET_LEASE_BY_PATH,
    // DFSTestUtil
    WAIT_CORRUPT_REPLICAS,
    // NNThroughputBenchmark
    ADD_INODE,
    // TestNodeCount
    TEST_NODE_COUNT,
    // Transaction in unit tests.
    TEST,
  }
  protected static Log log = LogFactory.getLog(RequestHandler.class);
  protected Object[] params = null;
  public static final int RETRY_COUNT = 1;
  protected OperationType opType;

  public RequestHandler(OperationType opType) {
    this.opType = opType;
  }

  public Object handle() throws IOException {
    return run(false, false, null);
  }

  public Object handle(Namesystem namesystem) throws IOException {
    return run(false, false, namesystem);
  }
   
  public Object handleWithWriteLock(Namesystem namesystem) throws IOException {
    return run(true, false, namesystem);
  }

  public Object handleWithReadLock(Namesystem namesystem) throws IOException {
    return run(false, true, namesystem);
  }
  
  protected abstract Object run(boolean writeLock, boolean readLock, Namesystem namesystem) throws IOException;
  
  public abstract Object performTask() throws PersistanceException, IOException;

  public RequestHandler setParams(Object... params) {
    this.params = params;
    return this;
  }

  public Object[] getParams() {
    return this.params;
  }
}
