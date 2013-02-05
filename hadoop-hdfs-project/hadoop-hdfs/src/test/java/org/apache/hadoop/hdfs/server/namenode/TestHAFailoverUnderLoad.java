package org.apache.hadoop.hdfs.server.namenode;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

import org.apache.log4j.Level;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 *
 * @author Jude
 * Tests the failover during load (read/write operations) when:
 * 1. Failover from Leader to namenode with next lowest Id
 * 2. When any other non-leader namenode crashes
 * 3. When datanodes crash
 * 
 * In each of the test case, the following should be ensured
 * 1. The load should continue to run during failover
 * 2. No corrupt blocks are detected during load
 * 3. Failovers should perform successfully from the Leader namenode to the namenode with the next lowest id
 */
public class TestHAFailoverUnderLoad extends junit.framework.TestCase {

  public static final Log LOG = LogFactory.getLog(TestHAFailoverUnderLoad.class);

  {
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
  }
  Configuration conf = new HdfsConfiguration();
  MiniDFSCluster cluster = null;
  FileSystem fs = null;
  int NN1 = 0, NN2 = 1;
  static int NUM_NAMENODES = 2;
  static int NUM_DATANODES = 6;
  // 10 seconds timeout default
  long timeout = 10000;
  Path dir = new Path("/testsLoad");
  //Writer[] writers = new Writer[10];
  Writer[] writers = new Writer[5];

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private void setUp(int replicationFactor) throws IOException {
    // initialize the cluster with minimum 2 namenodes and minimum 6 datanodes
    if (NUM_NAMENODES < 2) {
      NUM_NAMENODES = 2;
    }
    if (NUM_DATANODES < 6) {
      NUM_DATANODES = 6;
    }

    this.conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 1);
    //conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10 * 1000); // 10 sec
    //conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 3);  // 3 sec

    cluster = new MiniDFSCluster.Builder(conf).numNameNodes(NUM_NAMENODES).numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fs = cluster.getNewFileSystemInstance(NN1);

    timeout = conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_KEY, DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_DEFAULT)
            + conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;


    // create the directory namespace
    assertTrue(fs.mkdirs(dir));

    // create writers
    for (int i = 0; i < writers.length; i++) {
      writers[i] = new Writer(fs, new Path(dir, "file" + i));
    }


  }

  private void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private void startWriters() {
    for (int i = 0; i < writers.length; i++) {
      writers[i].start();
    }
  }

  private void stopWriters() throws InterruptedException {
    for (int i = 0; i < writers.length; i++) {
      if (writers[i] != null) {
        writers[i].running = false;
        writers[i].interrupt();
      }
    }
    for (int i = 0; i < writers.length; i++) {
      if (writers[i] != null) {
        writers[i].join();
      }
    }
  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private void verifyFile() throws IOException {
    LOG.info("Verify the file");
    for (int i = 0; i < writers.length; i++) {
      LOG.info(writers[i].filepath + ": length=" + fs.getFileStatus(writers[i].filepath).getLen());
      FSDataInputStream in = null;
      try {
        in = fs.open(writers[i].filepath);
        //for (int j = 0, x; (x = in.readInt()) != -1; j++) {
        boolean eof = false;
        int j = 0, x = 0;
        while (!eof) {
          try {
            x = in.readInt();
            assertEquals(j, x);
            j++;
          } catch (EOFException ex) {
            eof = true; // finished reading file
          }
        }
      } finally {
        IOUtils.closeStream(in);
      }
    }
  }

//  //@Test
//  public void testDeadlock() {
//    try {
//    setUp((short)3);
//    String storageId = cluster.getDataNodeByIndex(0).getDatanodeId().getStorageID();
//    Thread t1 = new DeadlockTester(NN1, storageId);
//    t1.start();
//    Thread t2 = new DeadlockTester(NN2, storageId);
//    t2.start();
//    
//    Thread.sleep(50000);
//    t1.interrupt();
//    t2.interrupt();
//    }
//    catch(Exception ex)
//    {
//      ex.printStackTrace();
//      fail(ex.getMessage());
//    }
//    finally {
//      shutdown();
//    }
//    
//  }
//  
//  public class DeadlockTester extends Thread{
//    
//    int nnIndex = 0;
//    String storageId;
//    public DeadlockTester(int nnIndex, String storageId) {
//      this.nnIndex = nnIndex;
//      this.storageId = storageId;
//    }
//    @Override
//    public void run() {
//      try {
//        for(;true;) {
//          cluster.getNamesystem(nnIndex).testDeadlock(storageId);
//        }
//      }
//      catch(Exception ex) {
//        
//      }
//    }
//  }
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /**
   * Under load perform failover by killing leader NN1
   * NN2 will be active and loads are now processed by NN2
   * Load should still continue
   * No corrupt blocks should be reported
   */
  @Test
  public void testFailoverWhenLeaderNNCrashes() {
    // Testing with replication factor of 3
    short repFactor = 3;
    LOG.info("Running test [testFailoverWhenLeaderNNCrashes()] with replication factor " + repFactor);
    testFailoverWhenLeaderNNCrashes(repFactor);
    // Testing with replication factor of 6
    repFactor = 6;
    LOG.info("Running test [testFailoverWhenLeaderNNCrashes()] with replication factor " + repFactor);
    testFailoverWhenLeaderNNCrashes(repFactor);
  }

  private void testFailoverWhenLeaderNNCrashes(short replicationFactor) {
    try {
      // setup the cluster with required replication factor
      setUp(replicationFactor);

      // save leader namenode port to restart with the same port
      int nnport = cluster.getNameNodePort(NN1);

      try {
        // writers start writing to their files
        startWriters();

        // Give all the threads a chance to create their files and write something to it
        Thread.sleep(10000); // 10 sec

        // kill leader NN1
        cluster.shutdownNameNode(NN1);
        TestHABasicFailover.waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2), timeout);
        // Check NN2 is the leader and failover is detected
        assertTrue("NN2 is expected to be the leader, but is not", cluster.getNameNode(NN2).isLeader());
        assertTrue("Not all datanodes detected the new leader", TestHABasicFailover.doesDataNodesRecognizeLeader(cluster.getDataNodes(), cluster.getNameNode(NN2)));

        // the load should still continue without any IO Exception thrown
        LOG.info("Wait a few seconds. Let them write some more");
        Thread.sleep(2000);

      } finally {
        stopWriters();
      }
      // the block report intervals would inform the namenode of under replicated blocks
      // hflush() and close() would guarantee replication at all datanodes. This is a confirmation
      waitReplication(fs, dir, replicationFactor, timeout);


      // restart the cluster without formatting using same ports and same configurations
      cluster.shutdown();
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnport).format(false).numNameNodes(NUM_NAMENODES).numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();

      // update the client so that it has the fresh list of namenodes. Black listed namenodes will be removed
      fs = cluster.getNewFileSystemInstance(NN1);

      verifyFile(); // throws IOException. Should be caught by parent
    } catch (Exception ex) {
      LOG.error("Received exception: " + ex.getMessage(), ex);
      ex.printStackTrace();
      fail("Exception: " + ex.getMessage());
    } finally {
      shutdown();
    }

  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /**
   * Under load kill Non-leader NN2 and restart.
   * Loads should still continue
   * No corrupt blocks should be reported
   */
  @Test
  public void testFailoverWhenNonLeaderNNCrashes() {
    // Testing with replication factor of 3
    short repFactor = 3;
    LOG.info("Running test [testFailoverWhenNonLeaderNNCrashes()] with replication factor " + repFactor);
    testFailoverWhenNonLeaderNNCrashes(repFactor);
    // Testing with replication factor of 6
    repFactor = 6;
    LOG.info("Running test [testFailoverWhenNonLeaderNNCrashes()] with replication factor " + repFactor);
    testFailoverWhenNonLeaderNNCrashes(repFactor);
  }

  private void testFailoverWhenNonLeaderNNCrashes(short replicationFactor) {
    try {
      // setup the cluster with required replication factor
      setUp(replicationFactor);

      // save leader namenode port to restart with the same port
      int nnport = cluster.getNameNodePort(NN1);

      try {
        // writers start writing to their files
        startWriters();

        // Give all the threads a chance to create their files and write something to it
        Thread.sleep(10000); // 10 sec

        // kill non-leader namenode NN2
        cluster.shutdownNameNode(NN2);
        // Check NN1 is still the leader
        assertTrue("NN1 is expected to be the leader, but is not", cluster.getNameNode(NN1).isLeader());
        assertTrue("Not all datanodes detect NN1 is the leader", TestHABasicFailover.doesDataNodesRecognizeLeader(cluster.getDataNodes(), cluster.getNameNode(NN1)));

        // the load should still continue without any IO Exception thrown
        LOG.info("Wait a few seconds. Let them write some more");
        Thread.sleep(100);

      } finally {
        stopWriters();
      }
      // the block report intervals would inform the namenode of under replicated blocks
      // hflush() and close() would guarantee replication at all datanodes. This is a confirmation
      waitReplication(fs, dir, replicationFactor, timeout);

      // restart the cluster without formatting using same ports and same configurations
      cluster.shutdown();
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnport).format(false).numNameNodes(NUM_NAMENODES).numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();

      // update the client so that it has the fresh list of namenodes. Black listed namenodes will be removed
      fs = cluster.getNewFileSystemInstance(NN1);

      verifyFile(); // throws IOException. Should be caught by parent
    } catch (Exception ex) {
      LOG.error("Received exception: " + ex.getMessage(), ex);
      ex.printStackTrace();
      fail("Exception: " + ex.getMessage());
    } finally {
      shutdown();
    }

  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  /**
   * Under load kill random datanodes after 15 seconds interval
   * Loads should still continue
   * No corrupt blocks should be reported
   * Also tests for successful pipeline recovery when a Datanode crashes
   */
  @Test
  // Problem with this test case is that, although the new pipeline is setup  correctly
  // when it creates the next pipeline and starts to stream, another datanode crashes!! and then another.. etc.
  // it only retries when it detects an error while streaming to the datanodes in the pipeline
  // but not when adding a new datanode to the pipeline
  //[https://issues.apache.org/jira/browse/HDFS-3436]
  public void testFailoverWhenDNCrashes() {
    // Testing with replication factor of 3
    short repFactor = 3;
    LOG.info("Running test [testFailoverWhenDNCrashes()] with replication factor " + repFactor);
    testFailoverWhenDNCrashes(repFactor);
    // Testing with replication factor of 6
    repFactor = 6;
    LOG.info("Running test [testFailoverWhenDNCrashes()] with replication factor " + repFactor);
    testFailoverWhenDNCrashes(repFactor);
  }

  public void testFailoverWhenDNCrashes(short replicationFactor) {

    StringBuffer sb = new StringBuffer();

    try {

      // reset this value
      NUM_DATANODES = 10;

      // setup the cluster with required replication factor
      setUp(replicationFactor);

      // save leader namenode port to restart with the same port
      int nnport = cluster.getNameNodePort(NN1);

      try {
        // writers start writing to their files
        startWriters();

        // Give all the threads a chance to create their files and write something to it
        Thread.sleep(10000); // 10 sec

        // kill some datanodes (but make sure that we have atleast 6+)
        // The pipleline should be broken and setup again by the client
        int numDatanodesToKill = 0;
        if (replicationFactor == 3) {
          numDatanodesToKill = 5;
        } else if (replicationFactor == 6) {
          numDatanodesToKill = 2;
        }

        sb.append("Killing datanodes ");
        for (int i = 0; i < numDatanodesToKill; i++) {
          // we need a way to ensure that atleast one valid replica is available
          int dnIndex = AppendTestUtil.nextInt(numDatanodesToKill);
          sb.append(cluster.getDataNodeByIndex(dnIndex).getSelfAddr()).append("\t");
          cluster.stopDataNode(dnIndex);

          // wait for 15 seconds then kill the next datanode
          // New pipeline recovery takes place
          try {
            Thread.sleep(15000);
          } catch (InterruptedException ex) {
          }
        }
        LOG.info(sb);

        // the load should still continue without any IO Exception thrown
        LOG.info("Wait a few seconds. Let them write some more");
        Thread.sleep(2000);

      } finally {
        // closing files - finalize all the files UC
        stopWriters();
      }

      // the block report intervals would inform the namenode of under replicated blocks
      // hflush() and close() would guarantee replication at all datanodes. This is a confirmation
      waitReplication(fs, dir, replicationFactor, timeout);

      // restart cluster - If DNs come up with different host:port, it will be replaced in [ReplicaUC]
      cluster.shutdown();
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnport).format(false).numNameNodes(NUM_NAMENODES).numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();

      // update the client so that it has the fresh list of namenodes. Black listed namenodes will be removed
      fs = cluster.getNewFileSystemInstance(NN1);

      // make sure that DNs that were killed are not part of the "expected block locations" or "triplets"
      // the blocks associated to the killed DNs should have been replicated
      verifyFile(); // throws IOException. Should be caught by parent
    } catch (Exception ex) {
      LOG.error("Received exception: " + ex.getMessage() + ". DNs killed [" + sb + "]", ex);
      ex.printStackTrace();
      fail("Exception: " + ex.getMessage());
    } finally {
      shutdown();
    }

  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static void waitReplication(FileSystem fs, Path rootDir, short replicationFactor, long timeout) throws IOException, TimeoutException {
    FileStatus[] files = fs.listStatus(rootDir);
    for (int i = 0; i < files.length;) {
      try {
        // increasing timeout to take into consideration 'ping' time with failed namenodes
        // if the client fetches for block locations from a dead NN, it would need to retry many times and eventually this time would cause a timeout
        // to avoid this, we set a larger timeout
        long expectedRetyTime = 20000; // 20seconds
        timeout = timeout + expectedRetyTime;
        DFSTestUtil.waitReplicationWithTimeout(fs, files[i].getPath(), replicationFactor, timeout);
        i++;
      } catch (ConnectException ex) {
        // ignore
        LOG.warn("Received Connect Exception (expected due to failure of NN)");
      }
    }
  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private static void waitTillFilesClose(FileSystem fs, Path rootDir, long timeout) throws IOException, TimeoutException, PersistanceException {
    long initTime = System.currentTimeMillis();

    //getting parent directory
    INode parent = EntityManager.find(INode.Finder.ByNameAndParentId, rootDir.getName(), 0);
//      INode parent = INodeHelper.getINode(rootDir.getName(), 0);
//      List<INode> files = INodeHelper.getChildren(parent.id);
    List<INode> files = (List<INode>) EntityManager.findList(INode.Finder.ByParentId, parent.getId());

    // loop through all files and check if they are closed. Expected to be closed by now
    for (int i = 0; i < files.size();) {

      if (!files.get(i).isUnderConstruction()) {
        i++;
      }

      if (System.currentTimeMillis() - initTime > timeout) {
        throw new TimeoutException("File [" + files.get(i).getFullPathName() + "] has not been closed. Still under construction");
      }
    }
  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  static class Writer extends Thread {

    FileSystem fs;
    final Path filepath;
    boolean running = true;
    FSDataOutputStream out = null;

    Writer(FileSystem fs, Path filepath) {
      super(Writer.class.getSimpleName() + ":" + filepath);
      this.fs = fs;
      this.filepath = filepath;

      // creating the file here
      try {
        out = this.fs.create(filepath);
      } catch (Exception ex) {
        LOG.info(getName() + " unable to create file [" + filepath + "]" + ex, ex);
        if (out != null) {
          IOUtils.closeStream(out);
          out = null;
        }
      }
    }

    public void run() {

      int i = 0;
      if (out != null) {
        try {
          for (; running; i++) {
            out.writeInt(i);
            out.hflush();
            sleep(100);
          }
        } catch (Exception e) {
          LOG.info(getName() + " dies: e=" + e, e);
        } finally {
          LOG.info(getName() + ": i=" + i);
          IOUtils.closeStream(out);
          //IOUtils.cleanup(LOG, out);  
          //try {
          //out.close();
          //}
          //catch(IOException ex) {
          //  LOG.error("*unable to close file. Exception: "+ex.getMessage(), ex);
          //}
        }//end-finally
      }// end-outcheck
      else {
        LOG.info(getName() + " outstream was null for file  [" + filepath + "]");
      }
    }//end-run        
  }//end-method
}