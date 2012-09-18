package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.log4j.NDC;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class TransactionalRequestHandler {

  public enum OperationType {
    // NameNodeRpcServer

    INITIALIZE, ACTIVATE, META_SAVE, SET_PERMISSION, SET_OWNER,
    GET_BLOCK_LOCATIONS, GET_STATUS, CONCAT, SET_TIMES, CREATE_SYM_LINK, GET_PREFERRED_BLOCK_SIZE,
    SET_REPLICATION, START_FILE, RECOVER_LEASE, APPEND_FILE, GET_ADDITIONAL_BLOCK,
    GET_ADDITIONAL_DATANODE, ABANDON_BLOCK, COMPLETE_FILE, RENAME_TO, RENAME_TO2, 
    DELETE, GET_FILE_INFO, MKDIRS, GET_CONTENT_SUMMARY, SET_QUOTA,
    FSYNC, COMMIT_BLOCK_SYNCHRONIZATION, RENEW_LEASE, GET_LISTING, REGISTER_DATANODE,
    HANDLE_HEARTBEAT, GET_MISSING_BLOCKS_COUNT, SAVE_NAMESPACE, SAFE_MODE_MONITOR,
    SET_SAFE_MODE, GET_BLOCKS_TOTAL, PROCESS_DISTRIBUTED_UPGRADE, GET_FS_STATE,
    UPDATE_BLOCK_FOR_PIPELINE, UPDATE_PIPELINE, LIST_CORRUPT_FILE_BLOCKS,
    GET_DELEGATION_TOKEN, RENEW_DELEGATION_TOKEN, CANCEL_DELEGATION_TOKEN,
    GET_SAFE_MODE, GET_NUMBER_OF_MISSING_BLOCKS, GET_PENDING_DELETION_BLOCKS_COUNT, GET_EXCESS_BLOCKS_COUNT,
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
    LEASE_MANAGER_MONITOR,
    //NamenodeJspHElper
    GET_SAFE_MODE_TEXT, GENERATE_HEALTH_REPORT, GET_INODE, TO_XML_BLOCK_INFO,
    TO_XML_CORRUPT_BLOCK_INFO,
    //MiniDfsCluster
    IS_NAMENODE_UP,
    // TestLease
    HAS_LEASE,
    // TestMissingBlocksAlert
    GET_UNDER_REPLICATED_NOT_MISSING_BLOCKS, GET_UNDER_REPLICATED_NOT_MISSING_BLOCKS2,
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
    TEST_NODE_COUNT,;
  }
  private static Log log = LogFactory.getLog(TransactionalRequestHandler.class);
  private ThreadLocal<Object[]> params = new ThreadLocal<Object[]>();
  public static final int RETRY_COUNT = 1;
  private OperationType opType;

  public TransactionalRequestHandler(OperationType opType) {
    this.opType = opType;
  }

  public Object handle() throws IOException {
    return run(false, false, null);
  }

  public Object handleWithWriteLock(Namesystem namesystem) throws IOException {
    return run(true, false, namesystem);
  }

  public Object handleWithReadLock(Namesystem namesystem) throws IOException {
    return run(false, true, namesystem);
  }

  private Object run(boolean writeLock, boolean readLock, Namesystem namesystem) throws IOException {
    if (writeLock) {
      namesystem.writeLock();
    }
    if (readLock) {
      namesystem.readLock();
    }
    boolean retry = true;
    boolean rollback = false;
    int tryCount = 0;
    IOException exception = null;

    try {
      while (retry && tryCount < RETRY_COUNT) {
        retry = true;
        rollback = false;
        tryCount++;
        exception = null;
        try {
          NDC.push(opType.name()); // Defines a context for every operation to track them in the logs easily.

          EntityManager.begin();
          return performTask();
        } catch (TransactionContextException ex) {
          log.error("Could not perfortm task", ex);
          rollback = true;
          retry = false;
        } catch (PersistanceException ex) {
          log.error("Could not perfortm task", ex);
          rollback = true;
          retry = true;
        } catch (IOException ex) {
          exception = ex;
        } finally {
          try {
            if (!rollback) {
              EntityManager.commit();
            }
          } catch (StorageException ex) {
            log.error("Could not commit transaction", ex);
            rollback = true;
            retry = true;
          } finally {
            try {
              if (rollback) {
                try {
                  EntityManager.rollback();
                } catch (StorageException ex) {
                  log.error("Could not rollback transaction", ex);
                }
              }
              if (tryCount == RETRY_COUNT && exception != null) {
                throw exception;
              }
            } finally {
              NDC.pop();
            }
          }
        }
      }
    } finally {
      if (writeLock) {
        namesystem.writeUnlock();
      }
      if (readLock) {
        namesystem.readUnlock();
      }
    }
    return null;
  }

  public abstract Object performTask() throws PersistanceException, IOException;

  private void checkParamsForThread() {
    if (params.get() == null) {
      Object[] prmtrs = new Object[8];
      params.set(prmtrs);
    }
  }

  public Object getParam1() {
    checkParamsForThread();
    return params.get()[0];
  }

  public TransactionalRequestHandler setParam1(Object param1) {
    checkParamsForThread();
    this.params.get()[0] = param1;
    return this;
  }

  public Object getParam2() {
    checkParamsForThread();
    return params.get()[1];
  }

  public TransactionalRequestHandler setParam2(Object param2) {
    checkParamsForThread();
    this.params.get()[1] = param2;
    return this;
  }

  public Object getParam3() {
    checkParamsForThread();
    return params.get()[2];
  }

  public TransactionalRequestHandler setParam3(Object param3) {
    checkParamsForThread();
    this.params.get()[2] = param3;
    return this;
  }

  public Object getParam4() {
    checkParamsForThread();
    return params.get()[3];
  }

  public TransactionalRequestHandler setParam4(Object param4) {
    checkParamsForThread();
    this.params.get()[3] = param4;
    return this;
  }

  public Object getParam5() {
    checkParamsForThread();
    return params.get()[4];
  }

  public TransactionalRequestHandler setParam5(Object param5) {
    checkParamsForThread();
    this.params.get()[4] = param5;
    return this;
  }

  public Object getParam6() {
    checkParamsForThread();
    return params.get()[5];
  }

  public TransactionalRequestHandler setParam6(Object param6) {
    checkParamsForThread();
    this.params.get()[5] = param6;
    return this;
  }

  public TransactionalRequestHandler setParam7(Object param7) {
    checkParamsForThread();
    this.params.get()[6] = param7;
    return this;
  }

  public Object getParam7() {
    checkParamsForThread();
    return params.get()[6];
  }

  public TransactionalRequestHandler setParam8(Object param8) {
    checkParamsForThread();
    this.params.get()[7] = param8;
    return this;
  }

  public Object getParam8() {
    checkParamsForThread();
    return params.get()[7];
  }
}
