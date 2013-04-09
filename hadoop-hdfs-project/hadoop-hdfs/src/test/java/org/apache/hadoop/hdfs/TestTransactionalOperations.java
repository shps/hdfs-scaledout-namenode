package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TestTransactionalOperations {

  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int bufferSize = 4096;
  private static final long SHORT_LEASE_PERIOD = 300L;
  private static final long LONG_LEASE_PERIOD = 3600000L;
  private final int seed = 28;
  static private String fakeUsername = "fakeUser1";
  static private String fakeGroup = "supergroup";

  @Test
  public void testMkdirs() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      Path exPath = new Path("/e1/e2");
      dfs.mkdirs(exPath);
      assert dfs.exists(exPath) : String.format("The path %s is not created.", exPath.toString());
      Path newPath = new Path(exPath.toString() + "/n3/n4");
      dfs.mkdirs(newPath);
      assert dfs.exists(newPath) : String.format("The path %s is not created.", newPath.toString());
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Tries different possible scenarios on startFile operation.
   * @throws IOException
   * @throws InterruptedException 
   */
  @Test
  public void testStartFile() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      DFSClient client = dfs.getDefaultDFSClient();
      Path exPath = new Path("/e1/e2");
      // Creates a path
      dfs.mkdirs(exPath, FsPermission.getDefault());
      assert dfs.exists(exPath) : String.format("Path %s does not exist.", exPath.toString());

      //Creates a file on an existing path
      Path f1 = new Path(exPath.toString() + "/f1");
      FSDataOutputStream stm = dfs.create(f1, true, bufferSize, (short) 2, blockSize);
      assert dfs.exists(f1) : String.format("Path %s does not exist.", f1.toString());
      stm.close();

      //Creates a file on a non-existing path
      Path f2 = new Path(exPath.toString() + "/e3/e4/f2");
      stm = dfs.create(f2, true, bufferSize, (short) 2, blockSize);
      assert dfs.exists(f2) : String.format("Path %s does not exist.", f2.toString());

      // This writes some blocks to the file and removes the last block in order to keep the lease on the file plus 
      // to have all the blocks as complete blocks.
      writeFile(stm, numBlocks * blockSize);
      stm.hflush();
      // Scenario "finalizeInodeFileUnderConstruction": Here we try to abandon the last block in order to have only complete blocks
      LocatedBlocks blocks = client.getNamenode().getBlockLocations(f2.toString(), 0, numBlocks * blockSize);
      ExtendedBlock lastBlock = blocks.getLastLocatedBlock().getBlock();
      client.getNamenode().abandonBlock(lastBlock, f2.toString(), client.getClientName());
      waitLeaseRecovery(cluster, SHORT_LEASE_PERIOD, LONG_LEASE_PERIOD);

      DistributedFileSystem dfs2 = (DistributedFileSystem) getFSAsAnotherUser(conf, fakeUsername, fakeGroup);
      // rewrite f2
      FSDataOutputStream stm2 = dfs2.create(f2, true, bufferSize, (short) 2, blockSize);
      assert dfs2.exists(f2) : String.format("Path %s does not exist.", f2.toString());

      //Scenario4: This time the file is under-cosntruction but the soft-lease of the holder expires and another
      //client tries to overwrite the file. Since the last block is under-construction, This makes the namennode to runs 
      //block-recovery for this file in place of making the file a complete file.
      writeFile(stm2, numBlocks * blockSize);
      stm2.hflush();
      waitLeaseRecovery(cluster, SHORT_LEASE_PERIOD, LONG_LEASE_PERIOD);
      try {
        dfs.create(f2, true, bufferSize, (short) 2, blockSize);
        assert false : "It must through RecoveryInProgressException.";
      } catch (IOException e) {
        // Good
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testCompleteFile() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      // Here we create a file with 3 replications while we have 2 datanode and min replication is 1.
      // This shouldn't make any problem. But this makes the namenode to add the block to the 
      // under-replicated blocks list.
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      Path f1 = new Path("/ed1/ed2/testCompleteFile");
      dfs.mkdirs(f1.getParent(), FsPermission.getDefault());
      assert dfs.exists(f1.getParent()) : String.format("The path %s does not exist.", f1.getParent().toString());
      FSDataOutputStream stm = dfs.create(f1, false, bufferSize, (short) 3, blockSize);
      writeFile(stm, blockSize);
      stm.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockReceivedAndDeleted() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      NamenodeProtocols namenode = cluster.getNameNodeRpc();
      DatanodeProtocol dnp = cluster.getNameNodeRpc();
      List<DataNode> dns = cluster.getDataNodes();
      Path f1 = new Path("/ed1/ed2/f1");
      Path f2 = new Path("/ed1/ed2/f2");
      dfs.mkdirs(f1.getParent(), FsPermission.getDefault());
      assert dfs.exists(f1.getParent()) : String.format("The path %s does not exist.", f1.getParent().toString());
      // Create two files, for one we report both replicas removed and for the other one we report one replica removed 
      // so we expect for the first file the blockinfo to be removed and for the latter the block-info be added to under-replicated blocks.
      DFSClient client = dfs.getDefaultDFSClient();
      namenode.create(f1.toString(), FsPermission.getDefault(), client.getClientName(),
              new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 2, blockSize);
      namenode.create(f2.toString(), FsPermission.getDefault(), client.getClientName(),
              new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 2, blockSize);
      LocatedBlock b1 = namenode.addBlock(f1.toString(), client.getClientName(), null,
              new DatanodeInfo[]{new DatanodeInfo(dns.get(1).getDatanodeId())});

      String bpId = cluster.getNamesystem().getBlockPoolId();
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
      LocatedBlock b2 = namenode.addBlock(f1.toString(), client.getClientName(), b1.getBlock(),
              new DatanodeInfo[]{new DatanodeInfo(dns.get(1).getDatanodeId())});
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b2.getBlock().getLocalBlock(), "")});
      Thread.sleep(300L);
      namenode.complete(f1.toString(), client.getClientName(), b2.getBlock());
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(1), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(2), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(1), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b2.getBlock().getLocalBlock(), "")});
      namenode.reportBadBlocks(new LocatedBlock[]{new LocatedBlock(b1.getBlock(),
                new DatanodeInfo[]{new DatanodeInfo(dns.get(2).getDatanodeId())})});

      LocatedBlock b3 = namenode.addBlock(f2.toString(), client.getClientName(),
              null, new DatanodeInfo[]{new DatanodeInfo(dns.get(1).getDatanodeId())}); //FIXME [H]: Create a scenario in which b3 is commited and completes in the receivedAndDeletedBlocks
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b3.getBlock().getLocalBlock(), "")});
      namenode.addBlock(f2.toString(), client.getClientName(), b3.getBlock(), null); // to make the b3 commited
      // We expect to send an excessive replica here.
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(2), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "-"),
                new ReceivedDeletedBlockInfo(b2.getBlock().getLocalBlock(), ""),
                new ReceivedDeletedBlockInfo(b3.getBlock().getLocalBlock(), "")});
    } catch (InterruptedException ex) {
      Logger.getLogger(TestTransactionalOperations.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetFileInfoAndContentSummary() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      Path f1 = new Path("/ed1/ed2/f1");
      dfs.mkdirs(f1.getParent(), FsPermission.getDefault());
      assert dfs.exists(f1.getParent()) : String.format("The path %s does not exist.", f1.getParent().toString());
      DFSTestUtil.createFile(dfs, f1, 2 * blockSize, (short) 2, 28L);
      assert dfs.exists(f1) : String.format("The file %s does not exist.", f1.toString());
      dfs.getFileStatus(f1);
      dfs.getContentSummary(f1);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetBlockLocations() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    conf.set(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, "1"); // To make the access time percision expired by default it is one hour
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      Path f1 = new Path("/ed1/ed2/f1");
      dfs.mkdirs(f1.getParent(), FsPermission.getDefault());
      assert dfs.exists(f1.getParent()) : String.format("The path %s does not exist.",
              f1.getParent().toString());
      FSDataOutputStream stm = dfs.create(f1, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm.hflush();
      assert dfs.getDefaultDFSClient().getBlockLocations(f1.toString(), 0, 2 * blockSize).length == 2;

    } finally {
      cluster.shutdown();
    }
  }

  /**
   * In this test case we create two files f1 (replication factor 3) and f2 (replication factor 2). There 
   * two datanodes. We make the lease-manager for f1 to finalize the file. and for f2 to reassign the lease
   * for it. 
   * @throws IOException 
   */
  @Test
  public void testLeaseManagerMonitor() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    conf.set(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, "1"); // To make the access time percision expired by default it is one hour
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      try {
        // We create three dfs for three different user names.
        DistributedFileSystem dfs1 = (DistributedFileSystem) cluster.getFileSystem();
        DistributedFileSystem dfs2 = (DistributedFileSystem) getFSAsAnotherUser(conf, "fake1", fakeGroup);

        Path f1 = new Path("/ed1/ed2/f1");
        Path f2 = new Path("/ed1/ed2/f2");
        dfs1.mkdirs(f1.getParent(), FsPermission.getDefault());
        NamenodeProtocols namenode = cluster.getNameNodeRpc();
        String bpId = cluster.getNamesystem().getBlockPoolId();
        namenode.create(f1.toString(), FsPermission.getDefault(), dfs1.getDefaultDFSClient().getClientName(),
                new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 3, blockSize);
        namenode.create(f2.toString(), FsPermission.getDefault(), dfs2.getDefaultDFSClient().getClientName(),
                new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 2, blockSize);

        List<DataNode> dns = cluster.getDataNodes();
        LocatedBlock b1 = namenode.addBlock(f1.toString(), dfs1.getDefaultDFSClient().getClientName(),
                null, null);
        namenode.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), fakeGroup,
                new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
        namenode.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(1), bpId), fakeGroup,
                new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
        LocatedBlock b2 = namenode.addBlock(f1.toString(), dfs1.getDefaultDFSClient().getClientName(),
                b1.getBlock(), null);
        namenode.abandonBlock(b2.getBlock(), f1.toString(), dfs1.getDefaultDFSClient().getClientName());
        LocatedBlock b3 = namenode.addBlock(f2.toString(), dfs2.getDefaultDFSClient().getClientName(),
                null, null);
        namenode.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), fakeGroup,
                new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b3.getBlock().getLocalBlock(), "")});

        // expire the hard-lease
        waitLeaseRecovery(cluster, SHORT_LEASE_PERIOD, SHORT_LEASE_PERIOD);

      } catch (InterruptedException ex) {
        assert false : ex.getMessage();
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testTransactionLockManager() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      DistributedFileSystem dfs2 = (DistributedFileSystem) getFSAsAnotherUser(conf, fakeUsername, fakeGroup);
      Path f1 = new Path("/ed1/ed2/f1");
      Path f2 = new Path("/ed1/ed2/f2");
      Path f3 = new Path("/ed1/ed2/f3");
      Path f4 = new Path("/ed1/ed2/ed3/f4");
      Path f5 = new Path("/ed1/ed2/ed4/f5");
      Path f6 = new Path("/ed1/ed2/ed4/ed5/f6");
      Path f7 = new Path("/ed1/ed2/ed4/ed5/f7");
      dfs.mkdirs(f1.getParent(), FsPermission.getDefault());
      dfs.mkdirs(f7.getParent(), FsPermission.getDefault());
      assert dfs.exists(f1.getParent()) : String.format("The path %s does not exist.",
              f1.getParent().toString());
      FSDataOutputStream stm = dfs.create(f1, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm.hflush();
      stm = dfs.create(f2, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm.hflush();
      stm = dfs.create(f3, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm.hflush();
      assert dfs.getDefaultDFSClient().getBlockLocations(f1.toString(), 0, 2 * blockSize).length == 2;

      FSDataOutputStream stm2 = dfs2.create(f4, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm2.hflush();
      stm2 = dfs2.create(f5, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm2.hflush();
      stm2 = dfs2.create(f6, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm2.hflush();
      stm2 = dfs2.create(f7, false, bufferSize, (short) 2, blockSize);
      writeFile(stm, 2 * blockSize);
      stm2.hflush();
      // Acquire locks for the getAdditionalBlocks

      try {
        // GetAdditionalBlock
        EntityManager.begin();

        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f1.toString()});
        tla.addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.WRITE).
                addExcess(TransactionLockManager.LockType.WRITE).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                acquire();

        EntityManager.commit();

        // Complete
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f1.toString()});
        tla.addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                acquire();

        EntityManager.commit();

        // GetFileInfo and GetContentSummary
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.READ,
                new String[]{f1.toString()});
        tla.addBlock(TransactionLockManager.LockType.READ).
                acquire();

        EntityManager.commit();

        // GetBlockLocations
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE, new String[]{f1.toString()});
        tla.addBlock(TransactionLockManager.LockType.READ).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                acquire();

        EntityManager.commit();

        // recoverLease
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f1.toString()});
        tla.addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE, "IamAnotherHolder").
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE).
                acquire();

        EntityManager.commit();

        //RenewLease
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addLease(TransactionLockManager.LockType.WRITE, "IamAHolder").
                acquire();
        EntityManager.commit();

        // GetAdditionalDataNode
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.READ,
                new String[]{f1.toString()}).
                addLease(TransactionLockManager.LockType.READ).
                acquire();

        EntityManager.commit();

        // Set_Permission, Set_Owner, SET_TIMES
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f1.toString()}).
                addBlock(TransactionLockManager.LockType.READ).
                acquire();

        EntityManager.commit();

        // GET_PREFFERED_BLOCK_SIZE
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.READ,
                new String[]{f1.toString()}).
                acquire();

        EntityManager.commit();

        //Append_file
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f1.toString()});
        tla.addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE, "IamAnotherHolder").
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE).
                addInvalidatedBlock(TransactionLockManager.LockType.WRITE).
                acquire();

        EntityManager.commit();

        // SET_QUOTA
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f1.toString()}).
                acquire();
        EntityManager.commit();

        // SET_REPLICATION

        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
                new String[]{f1.toString()}).
                addBlock(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE).
                acquire();
        EntityManager.commit();

        // CONCAT

        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
                new String[]{f1.toString(), f2.toString(), f3.toString()}).
                addBlock(TransactionLockManager.LockType.WRITE).
                acquire();
        EntityManager.commit();

        // GET_LISTING

        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN,
                TransactionLockManager.INodeLockType.READ,
                new String[]{f1.getParent().toString()}).
                addBlock(TransactionLockManager.LockType.READ).
                addReplica(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                acquire();
        EntityManager.commit();

        // DELETE

        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f1.getParent().toString()}).
                addLease(TransactionLockManager.LockType.WRITE, "ZzZzZz").
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addBlock(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.WRITE).
                addCorrupt(TransactionLockManager.LockType.WRITE).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                acquire();
        EntityManager.commit();

        // MKDIR , CREATE_SYM_LINK
        Path f8 = new Path(f1.getParent().toString() + "/nd1/nd2/f8");
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH_WITH_UNKNOWN_HEAD,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{f8.getParent().toString()}).
                acquire();
        EntityManager.commit();

        // START_FILE
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH_WITH_UNKNOWN_HEAD,
                TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
                new String[]{f7.toString()}).
                addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE, "1234Holder").
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.WRITE).
                addCorrupt(TransactionLockManager.LockType.WRITE).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                acquire();
        EntityManager.commit();

        LocatedBlocks locatedBlocks = cluster.getNameNodeRpc().getBlockLocations(f1.toString(), 0, blockSize);
        assert locatedBlocks.getLocatedBlocks().size() > 0;
        // BLOCK_RECEIVED_AND_DELETED
        long bid = locatedBlocks.getLocatedBlocks().get(0).getBlock().getBlockId();
        EntityManager.begin();
        tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeLockType.READ).
                addBlock(TransactionLockManager.LockType.WRITE, bid).
                addReplica(TransactionLockManager.LockType.WRITE).
                addExcess(TransactionLockManager.LockType.WRITE).
                addCorrupt(TransactionLockManager.LockType.WRITE).
                addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE).
                addPendingBlock(TransactionLockManager.LockType.WRITE).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                addInvalidatedBlock(TransactionLockManager.LockType.READ).
                acquireByBlock();
        EntityManager.commit();

        // HANDLE_HEARTBEAT
        EntityManager.begin();
        TransactionLockAcquirer.acquireLockList(TransactionLockManager.LockType.READ_COMMITTED, ReplicaUnderConstruction.Finder.ByBlockId, bid);
        TransactionLockAcquirer.acquireLockList(TransactionLockManager.LockType.READ_COMMITTED, UnderReplicatedBlock.Finder.All);
        TransactionLockAcquirer.acquireLockList(TransactionLockManager.LockType.READ_COMMITTED, PendingBlockInfo.Finder.All);
        TransactionLockAcquirer.acquireLockList(TransactionLockManager.LockType.READ_COMMITTED, BlockInfo.Finder.All);
        TransactionLockAcquirer.acquireLockList(TransactionLockManager.LockType.READ_COMMITTED, InvalidatedBlock.Finder.All);
        EntityManager.commit();

        // LEASE_MANAGER
        EntityManager.begin();
        TransactionLockManager tlm = new TransactionLockManager();
        tlm.addINode(TransactionLockManager.INodeLockType.WRITE).
                addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE, dfs.getDefaultDFSClient().clientName).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                acquireByLease();
        EntityManager.commit();
      } catch (PersistanceException ex) {
        Logger.getLogger(TestTransactionalOperations.class.getName()).log(Level.SEVERE, null, ex);
        assert false : ex.getMessage();
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testTransactionLockAcquirer() throws IOException, StorageException, UnresolvedPathException, PersistanceException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      Path p1 = new Path("/ed1/ed2/");
      Path p2 = new Path("/ed1/ed2/ed3/ed4");
      dfs.mkdirs(p2, FsPermission.getDefault());
      assert dfs.exists(p2);

      EntityManager.begin();
      // first get write lock on ed2
      LinkedList<INode> lockedINodes = TransactionLockAcquirer.acquireInodeLockByPath(
              TransactionLockManager.INodeLockType.WRITE,
              p1.toString(),
              true);
      assert lockedINodes != null;
      assert lockedINodes.size() == 2; // first two path components
      lockedINodes = TransactionLockAcquirer.acquireLockOnRestOfPath(TransactionLockManager.INodeLockType.WRITE,
              lockedINodes.getLast(), p2.toString(), p1.toString(), true);
      assert lockedINodes != null && lockedINodes.size() == 2; // the other two
      EntityManager.commit();

      EntityManager.begin();
      // The same thing with write on parent
      lockedINodes = TransactionLockAcquirer.acquireInodeLockByPath(
              TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
              p1.toString(),
              true);
      assert lockedINodes != null;
      assert lockedINodes.size() == 2; // first two path components
      lockedINodes = TransactionLockAcquirer.acquireLockOnRestOfPath(TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
              lockedINodes.getLast(), p2.toString(), p1.toString(), true);
      assert lockedINodes != null && lockedINodes.size() == 2; // the other two
      EntityManager.commit();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testConcurrentWriteLocksOnTheSameRow() throws IOException, InterruptedException, PersistanceException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, "1");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      final NamenodeProtocols nameNodeProto = cluster.getNameNodeRpc();
      final DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      int numThreads = 100;
      Thread[] threads = new Thread[numThreads];
      final CyclicBarrier barrier = new CyclicBarrier(numThreads);
      final CountDownLatch latch = new CountDownLatch(numThreads);

      // create file on the root
      Runnable fileCreator = new Runnable() {

        @Override
        public void run() {
          String name = "/" + Thread.currentThread().getName();
          try {
            barrier.await(); // to make all threads starting at the same time
            nameNodeProto.create(name, FsPermission.getDefault(),
                    dfs.getDefaultDFSClient().clientName,
                    new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)), true, (short) 2, blockSize);
            latch.countDown();
          } catch (Exception ex) {
            latch.countDown();
            ex.printStackTrace();
          }
        }
      };
      for (int i = 0; i < numThreads; i++) {
        threads[i] = new Thread(fileCreator);
        threads[i].start();
      }

      latch.await();
      String root = "/";
      System.out.println("root = " + root);
      DirectoryListing list = nameNodeProto.getListing(root, new byte[]{}, false);

      assert list.getPartialListing().length == numThreads; // root must have 100 children
    } finally {
      cluster.shutdown();
    }
  }

  

  //
  // writes specified bytes to file.
  //
  public void writeFile(FSDataOutputStream stm, int size) throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, size);
    stm.write(buffer, 0, size);
  }

  void waitLeaseRecovery(MiniDFSCluster cluster, long softPeriod, long hardPeriod) {
    cluster.setLeasePeriod(softPeriod, hardPeriod);
    // wait for the lease to expire
    try {
      Thread.sleep(2 * 3000);  // 2 heartbeat intervals
    } catch (InterruptedException e) {
    }
  }

  private FileSystem getFSAsAnotherUser(final Configuration c, String username, String group)
          throws IOException, InterruptedException {
    return FileSystem.get(FileSystem.getDefaultUri(c), c,
            UserGroupInformation.createUserForTesting(username,
            new String[]{group}).getUserName());
  }
}
