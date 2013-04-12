package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.CorruptReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ExcessReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InodeDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeasePathDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaUnderConstruntionDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.UnderReplicatedBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class TestLongTransactions {

  public static final Log LOG = LogFactory.getLog(TestLongTransactions.class);
  HdfsConfiguration conf = null;
  boolean test_status = true; // means test has passed. in case of error threads will set it to false
  int wait_time = 10 * 1000;
  long start_time = 0;
  boolean test_started = false; // first thread will set it to true. 
  boolean locksheld =false;

  @Before
  public void initialize() throws Exception {

    conf = new HdfsConfiguration();
    StorageFactory.setConfiguration(conf);
    StorageFactory.getConnector().formatStorage();
  }

  @After
  public void close() {
  }

  @Test
  public void testDeleteOnExit() {
    try {

      
      for (int i = 0; i < 1; i++) {
        System.out.println("_________________________________________________");

        start_time = System.currentTimeMillis();
        // just put on row in a table and make two transactins 
        // wait on it 
        insertData();

        System.out.println("___________________DATA INSERTED______________________________");

        int waitTime;

//        Runnable w1 = new BlockRcvdAndDelete(OperationType.BLCK_RECVE_DEL_1);
//        Runnable w2 = new BlockRcvdAndDelete(OperationType.BLCK_RECVE_DEL_2);
        
        Runnable w1 = new FileComplete(OperationType.CF_1, 4000);
        Runnable w2 = new FileComplete(OperationType.CF_2, 0);

        Thread t1 = new Thread(w1);
 
        Thread t2 = new Thread(w2);



        t1.start();
        sleep(300);
        t2.start();

        t2.join();
        t1.join();

        if (!test_status) {
          fail("Test failed. Two transactions got the write lock at the same time");
        }
      }

    } catch (Exception e) // all exceptions are bad
    {
      e.printStackTrace();
      fail("Test Failed");
    }
  }
  
  protected void sleep(int interval) {
      try {
        Thread.sleep(interval);
      } catch (Exception e) {
      }
    }

  private void insertData() throws StorageException {
    System.out.println("Building the data...");
    List<INode> newFiles = new LinkedList<INode>();
    INode root = new INodeDirectoryWithQuota(INodeDirectory.ROOT_NAME,
            new PermissionStatus("salman", "usr", new FsPermission((short) 0755)),
            Integer.MAX_VALUE, FSDirectory.UNKNOWN_DISK_SPACE);
    root.setId(0);
    root.setParentId(-1);

    INodeFile file;
    file = new INodeFile(false, new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), (short) 3, 0l, 0l, 0l);
    file.setId(1);
    file.setName("testRecoverFinalized");
    file.setClientName("DFSClient_NONMAPREDUCE_-2064576337_8");
    file.setParentId(0);

    newFiles.add(root);
    newFiles.add(file);


    StorageFactory.getConnector().beginTransaction();
    InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
    da.prepare(new LinkedList<INode>(), newFiles, new LinkedList<INode>());
    StorageFactory.getConnector().commit();


    BlockInfo blk = new BlockInfo(new Block(1, 1, 1));
    blk.setINodeId(1);
    blk.setBlockIndex(0);
    List<BlockInfo> blkList = new LinkedList<BlockInfo>();
    blkList.add(blk);

    StorageFactory.getConnector().beginTransaction();
    BlockInfoDataAccess bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
    bda.prepare(new LinkedList<BlockInfo>(), blkList, new LinkedList<BlockInfo>());
    StorageFactory.getConnector().commit();


    Lease lease = new Lease("DFSClient_NONMAPREDUCE_-2064576337_8", 1893068665, 1365707470251L);
    List<Lease> leaseList = new LinkedList<Lease>();
    leaseList.add(lease);

    StorageFactory.getConnector().beginTransaction();
    LeaseDataAccess lda = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
    lda.prepare(new LinkedList<Lease>(), leaseList, new LinkedList<Lease>());
    StorageFactory.getConnector().commit();

    LeasePath lp = new LeasePath("/testRecoverFinalized", 1893068665);
    List<LeasePath> lpList = new LinkedList<LeasePath>();
    lpList.add(lp);


    StorageFactory.getConnector().beginTransaction();
    LeasePathDataAccess lpda = (LeasePathDataAccess) StorageFactory.getDataAccess(LeasePathDataAccess.class);
    lpda.prepare(new LinkedList<LeasePath>(), lpList, new LinkedList<LeasePath>());
    StorageFactory.getConnector().commit();


    IndexedReplica replica = new IndexedReplica(1, "DS1", 1);
    List<IndexedReplica> rList = new LinkedList<IndexedReplica>();
    rList.add(replica);


    StorageFactory.getConnector().beginTransaction();
    ReplicaDataAccess rda = (ReplicaDataAccess) StorageFactory.getDataAccess(ReplicaDataAccess.class);
    rda.prepare(new LinkedList<IndexedReplica>(), rList, new LinkedList<IndexedReplica>());
    StorageFactory.getConnector().commit();


    CorruptReplica cr = new CorruptReplica(1, "DS1");
    List<CorruptReplica> crList = new LinkedList<CorruptReplica>();
    crList.add(cr);

    StorageFactory.getConnector().beginTransaction();
    CorruptReplicaDataAccess crda = (CorruptReplicaDataAccess) StorageFactory.getDataAccess(CorruptReplicaDataAccess.class);
    crda.prepare(new LinkedList<CorruptReplica>(), crList, new LinkedList<CorruptReplica>());
    StorageFactory.getConnector().commit();



    ExcessReplica er = new ExcessReplica("DS1", 1);
    List<ExcessReplica> erList = new LinkedList<ExcessReplica>();
    erList.add(er);

    StorageFactory.getConnector().beginTransaction();
    ExcessReplicaDataAccess erda = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
    erda.prepare(new LinkedList<ExcessReplica>(), erList, new LinkedList<ExcessReplica>());
    StorageFactory.getConnector().commit();



    ReplicaUnderConstruction ruc = new ReplicaUnderConstruction(ReplicaState.FINALIZED, "DS1", 1, 0);
    List<ReplicaUnderConstruction> rucList = new LinkedList<ReplicaUnderConstruction>();
    rucList.add(ruc);

    StorageFactory.getConnector().beginTransaction();
    ReplicaUnderConstruntionDataAccess rucda = (ReplicaUnderConstruntionDataAccess) StorageFactory.getDataAccess(ReplicaUnderConstruntionDataAccess.class);
    rucda.prepare(new LinkedList<ReplicaUnderConstruction>(), rucList, new LinkedList<ReplicaUnderConstruction>());
    StorageFactory.getConnector().commit();


    UnderReplicatedBlock urb = new UnderReplicatedBlock(0, 1L);
    List<UnderReplicatedBlock> urList = new LinkedList<UnderReplicatedBlock>();
    urList.add(urb);

    StorageFactory.getConnector().beginTransaction();
    UnderReplicatedBlockDataAccess urbda = (UnderReplicatedBlockDataAccess) StorageFactory.getDataAccess(UnderReplicatedBlockDataAccess.class);
    urbda.prepare(new LinkedList<UnderReplicatedBlock>(), urList, new LinkedList<UnderReplicatedBlock>());
    StorageFactory.getConnector().commit();

  }

  private class Common {

    protected String name;

    public Common(String name) {
      this.name = name;
    }

    protected void beginTx() {
      try {
        printMsg("Tx Start");
        StorageFactory.getConnector().beginTransaction();
      } catch (StorageException e) {
        test_status = false;
        System.err.println("Test Failed Begin Tx ");
        e.printStackTrace();
        fail("Test Failed");
      }
    }

    protected void commitTx() {
      try {
        printMsg("Commiting");
        StorageFactory.getConnector().commit();
      } catch (StorageException e) {
        test_status = false;
        System.err.println("Test Failed Commit Tx ");
        e.printStackTrace();
        fail("Test Failed");
      }
    }

    protected INode readInodeRowWithWriteLock(InodeDataAccess da, long id) {
      try {

        StorageFactory.getConnector().writeLock();
        INode file = (INodeFile) da.findInodeById(id);
        printMsg("Write Lock " + "pid: " + file.getParentId() + ", id:" + file.getId() + ", name:" + file.getName());
        return file;
      } catch (StorageException e) {
        test_status = false;

        System.err.println("Test Failed ReadRowWithWriteLock. id " + id);
        e.printStackTrace();
        fail("Test Failed");
      }
      return null;
    }

    protected INode readInodeRowWithWriteLock(InodeDataAccess da, String name, long parent_id) {
      try {

        StorageFactory.getConnector().writeLock();
        INode file = (INodeFile) da.findInodeByNameAndParentId(name, parent_id);
        printMsg("ReadC Lock " + "pid: " + file.getParentId() + ", id:" + file.getId() + ", name:" + file.getName());
        return file;
      } catch (StorageException e) {
        test_status = false;

        System.err.println("Test Failed ReadRowWithWriteLock. name " + name + " paretn_id " + parent_id);
        e.printStackTrace();
        fail("Test Failed");
      }
      return null;
    }

    protected INode readInodeRowWithRCLock(InodeDataAccess da, long id) {
      try {

        StorageFactory.getConnector().readCommitted();
        INode file = (INodeFile) da.findInodeById(id);
        printMsg("ReadC Lock " + "pid: " + file.getParentId() + ", id:" + file.getId() + ", name:" + file.getName());
        return file;
      } catch (StorageException e) {
        test_status = false;
        System.err.println("Test Failed ReadRowWithRCLock. id " + id + " exception ");
        e.printStackTrace();
        fail("Test Failed");
      }
      return null;
    }

    protected INode readInodeRowWithRCLock(InodeDataAccess da, String name, long parent_id) {
      try {

        StorageFactory.getConnector().readCommitted();
        INode file = (INodeFile) da.findInodeByNameAndParentId(name, parent_id);
        printMsg("ReadC Lock " + "pid: " + file.getParentId() + ", id:" + file.getId() + ", name:" + file.getName());
        return file;
      } catch (StorageException e) {
        test_status = false;
        System.err.println("Test Failed ReadRowWithRCLock. name " + name + " paretn_id " + parent_id + " exception ");
        e.printStackTrace();
        fail("Test Failed");
      }
      return null;
    }

    protected BlockInfo readBlockRowWithRCLock(BlockInfoDataAccess da, long id) {
      try {

        StorageFactory.getConnector().readCommitted();
        BlockInfo blk = (BlockInfo) da.findById(id);
        printMsg("Blk ReadC Lock " + "id: " + blk.getBlockId());
        return blk;
      } catch (StorageException e) {
        test_status = false;
        System.err.println("Test Failed readBlockRowWithRCLock. id " + id + " exception ");
        e.printStackTrace();
        fail("Test Failed");
      }
      return null;
    }

    protected void printReplicas(List<IndexedReplica> replicas) {
      for (int i = 0; i < replicas.size(); i++) {
        IndexedReplica replica = replicas.get(i);
        printMsg(name + " " + replica.toString());
      }
    }

    protected void printMsg(String msg) {
      System.out.println((System.currentTimeMillis() - start_time) + " " + " " + name + " " + msg);
    }

    protected void randomWait(int interval) {
      try {
        Random rand = new Random();
        rand.setSeed(System.currentTimeMillis());
        Thread.sleep(rand.nextInt(interval));
      } catch (Exception e) {
      }
    }
  }

  private class worker1 extends Common implements Runnable {

    public worker1(String name) {
      super(name);

    }

    @Override
    public void run() {
      //randomWait(10);
      beginTx();

      InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
      BlockInfoDataAccess bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);

      printMsg(" about to read blk table RC");
      readBlockRowWithRCLock(bda, 1);
      printMsg(" about to read inode table RC");
      readInodeRowWithRCLock(da, 1);
      printMsg(" about to read inode table WRITE");
      readInodeRowWithWriteLock(da, 0);
      randomWait(4000);
      commitTx();

    }
  }

  private class worker2 extends Common implements Runnable {

    public worker2(String name) {
      super(name);

    }

    @Override
    public void run() {

      randomWait(40);
      beginTx();

      InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
      printMsg(" about to Write lock");
      readInodeRowWithWriteLock(da, "/", -1);
      //randomWait(4000);   
      commitTx();


    }
  }

  private class FileComplete extends Common implements Runnable {

    OperationType opName;
    private int sleeptime;
    public FileComplete(OperationType name,int sleep) {
      super(name.toString());
      opName = name;
      sleeptime=sleep;
    }

    @Override
    public void run() {



      TransactionalRequestHandler completeFileHandler = new TransactionalRequestHandler(opName) {
        @Override
        public Object performTask() throws PersistanceException, IOException {
          
          log.debug("___________________PERFORM TASK COMP FILE______________________________");

          sleep(sleeptime);

          return null;
        }

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          log.debug("__________________ACQUIRE LOCKS COMP FILE_______________________________");
          TransactionLockManager tla = new TransactionLockManager();
          tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                  TransactionLockManager.INodeLockType.WRITE,
                  new String[]{"/testRecoverFinalized"});
          tla.addBlock(TransactionLockManager.LockType.WRITE).
                  addLease(TransactionLockManager.LockType.WRITE).
                  addLeasePath(TransactionLockManager.LockType.WRITE).
                  addReplica(TransactionLockManager.LockType.READ).
                  addCorrupt(TransactionLockManager.LockType.READ).
                  addExcess(TransactionLockManager.LockType.READ).
                  addReplicaUc(TransactionLockManager.LockType.WRITE).
                  addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE).
                  acquire();
                 
        }
      };




      try {
        completeFileHandler.handleWithWriteLock(null);
      } catch (Exception e) {

        printMsg("Error" + e);
        fail("Test Failed");
      }
    }
  }
  
  
    private class BlockRcvdAndDelete extends Common implements Runnable {

      OperationType opName;
      int sleeptime;
    public BlockRcvdAndDelete(OperationType name,int sleep) {
      super(name.toString());
      opName = name;
      sleeptime=sleep;
    }

    @Override
    public void run() {



     TransactionalRequestHandler blockReceivedAndDeletedHandler = new TransactionalRequestHandler(opName) {
                @Override
                public Object performTask() throws PersistanceException, IOException {
                  log.debug("___________________PERFORM TASKK BLK RCV AND DEL ______________________________");
                  sleep(sleeptime);
          return null;
                }

                @Override
                public void acquireLock() throws PersistanceException, IOException {
                  log.debug("__________________ACQUIRE LOCKS  BLK RCV AND DEL _______________________________");
                    Long blockID =  (Long)getParams()[0];
                    TransactionLockManager lm = new TransactionLockManager();
                    lm.addINode(TransactionLockManager.INodeLockType.WRITE).
                            addBlock(TransactionLockManager.LockType.WRITE, blockID).
                            addReplica(TransactionLockManager.LockType.WRITE).
                            addExcess(TransactionLockManager.LockType.WRITE).
                            addCorrupt(TransactionLockManager.LockType.WRITE).
                            addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE);
                    if (true) {
                        lm.addPendingBlock(TransactionLockManager.LockType.WRITE).
                                addReplicaUc(TransactionLockManager.LockType.WRITE).
                                addInvalidatedBlock(TransactionLockManager.LockType.READ);
                    }
                    lm.acquireByBlock();
                }
            };




      try {
        blockReceivedAndDeletedHandler.setParams(new Long(1));
        blockReceivedAndDeletedHandler.handle();
      } catch (Exception e) {

        printMsg("Error" + e);
        fail("Test Failed");
      }
    }
  }
}

