package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
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
      DFSClient client = dfs.dfs;
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
      namenode.create(f1.toString(), FsPermission.getDefault(), dfs.dfs.getClientName(),
              new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 2, blockSize);
      namenode.create(f2.toString(), FsPermission.getDefault(), dfs.dfs.getClientName(),
              new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 2, blockSize);
      LocatedBlock b1 = namenode.addBlock(f1.toString(), dfs.dfs.getClientName(), null,
              new DatanodeInfo[]{new DatanodeInfo(dns.get(1).getDatanodeId())});

      String bpId = cluster.getNamesystem().getBlockPoolId();
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
      LocatedBlock b2 = namenode.addBlock(f1.toString(), dfs.dfs.getClientName(), b1.getBlock(),
              new DatanodeInfo[]{new DatanodeInfo(dns.get(1).getDatanodeId())});
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b2.getBlock().getLocalBlock(), "")});
      Thread.sleep(300L);
      namenode.complete(f1.toString(), dfs.dfs.getClientName(), b2.getBlock());
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(1), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(2), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(1), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b2.getBlock().getLocalBlock(), "")});
      namenode.reportBadBlocks(new LocatedBlock[]{new LocatedBlock(b1.getBlock(),
                new DatanodeInfo[]{new DatanodeInfo(dns.get(2).getDatanodeId())})});

      LocatedBlock b3 = namenode.addBlock(f2.toString(), dfs.dfs.getClientName(),
              null, new DatanodeInfo[]{new DatanodeInfo(dns.get(1).getDatanodeId())}); //FIXME [H]: Create a scenario in which b3 is commited and completes in the receivedAndDeletedBlocks
      dnp.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), bpId,
              new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b3.getBlock().getLocalBlock(), "")});
      namenode.addBlock(f2.toString(), dfs.dfs.getClientName(), b3.getBlock(), null); // to make the b3 commited
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
      assert dfs.dfs.getBlockLocations(f1.toString(), 0, 2 * blockSize).length == 2;

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
        namenode.create(f1.toString(), FsPermission.getDefault(), dfs1.dfs.getClientName(),
                new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 3, blockSize);
        namenode.create(f2.toString(), FsPermission.getDefault(), dfs2.dfs.getClientName(),
                new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true, (short) 2, blockSize);

        List<DataNode> dns = cluster.getDataNodes();
        LocatedBlock b1 = namenode.addBlock(f1.toString(), dfs1.dfs.getClientName(),
                null, null);
        namenode.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(0), bpId), fakeGroup,
                new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
        namenode.blockReceivedAndDeleted(DataNodeTestUtils.getDNRegistrationForBP(dns.get(1), bpId), fakeGroup,
                new ReceivedDeletedBlockInfo[]{new ReceivedDeletedBlockInfo(b1.getBlock().getLocalBlock(), "")});
        LocatedBlock b2 = namenode.addBlock(f1.toString(), dfs1.dfs.getClientName(),
                b1.getBlock(), null);
        namenode.abandonBlock(b2.getBlock(), f1.toString(), dfs1.dfs.getClientName());
        LocatedBlock b3 = namenode.addBlock(f2.toString(), dfs2.dfs.getClientName(),
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
  public void testNormalLockAcquirer() throws IOException {
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
      assert dfs.dfs.getBlockLocations(f1.toString(), 0, 2 * blockSize).length == 2;

      // Acquire locks for the getAdditionalBlocks
      
      try {
        EntityManager.begin();

        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.addINode(TransactionLockAcquirer.INodeLockType.BY_PATH_LAST_WRITE_LOCK, f1.toString(), cluster.getNamesystem().getFsDirectory().getRootDir());
        tla.addBlock(TransactionLockAcquirer.LockType.WRITE, null).addLease(TransactionLockAcquirer.LockType.READ, null).addCorrupt(TransactionLockAcquirer.LockType.WRITE, null).addExcess(TransactionLockAcquirer.LockType.WRITE, null).addReplicaUc(TransactionLockAcquirer.LockType.WRITE, null);

        tla.acquire();
        EntityManager.commit();
      } catch (PersistanceException ex) {
        Logger.getLogger(TestTransactionalOperations.class.getName()).log(Level.SEVERE, null, ex);
        assert false : ex.getMessage();
      }
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
