package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;

import org.apache.log4j.Level;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

/**
 *
 * @author Jude This tests basic failover b/w two namenodes in the system
 */
public class TestHABasicFailover extends junit.framework.TestCase
{

    public static final Log LOG = LogFactory.getLog(TestHABasicFailover.class);

    
    {
        ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    }
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    int NUM_NAMENODES = 2;
    int NUM_DATANODES = 3;

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    /**
     * Testing basic failover. After starting namenodes NN1, NN2, the namenode
     * that first initializes itself would be elected the leader. We allow NN1
     * to be the leader. We kill NN1. Failover will start and NN2 will detect
     * failure of NN1 and hence would elect itself as the leader Also perform
     * fail-back to NN1 by killing NN2
     */
    @Test
    public void testFailover()
    {
 
        final int NN1 = 0, NN2 = 1;
        if (NUM_NAMENODES < 2)
        {
            NUM_NAMENODES = 2;
        }

        try
        {
            // Create cluster with 2 namenodes
            cluster = new MiniDFSCluster.Builder(conf).numNameNodes(NUM_NAMENODES).numDataNodes(NUM_DATANODES).build();
            cluster.waitActive();

            // Give it time for leader to be elected
            long timeout = conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_KEY, DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_DEFAULT)
                    + conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;

            /**
             * *********************************
             * testing fail over from NN1 to NN2
             * **********************************
             */
            // Check NN1 is the leader
            
            LOG.info("NameNode 1 id " + cluster.getNameNode(NN1).getId()+" address "+cluster.getNameNode(NN1).getServiceRpcAddress().toString() );
            LOG.info("NameNode 2 id " + cluster.getNameNode(NN2).getId()+" address "+cluster.getNameNode(NN2).getServiceRpcAddress().toString() );
            
            assertTrue("NN1 is expected to be leader, but is not", cluster.getNameNode(NN1).isLeader());

            // performing failover - Kill NN1. This would allow NN2 to be leader
            cluster.shutdownNameNode(NN1);
            
            

            // wait for leader to be elected and for Datanodes to also detect the leader
            waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2), timeout * 10);

            // Check NN2 is the leader and failover is detected
            assertTrue("NN2 is expected to be the leader, but is not", cluster.getNameNode(NN2).isLeader());
            assertTrue("Not all datanodes detected the new leader", doesDataNodesRecognizeLeader(cluster.getDataNodes(), cluster.getNameNode(NN2)));

            // restart the newly elected leader and see if it is still the leader
            cluster.restartNameNode(NN2);
            cluster.waitActive();
            waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2), timeout * 10);
            assertTrue("NN2 is expected to be the leader, but is not", cluster.getNameNode(NN2).isLeader());
            assertTrue("Not all datanodes detected the new leader", doesDataNodesRecognizeLeader(cluster.getDataNodes(), cluster.getNameNode(NN2)));

            /**
             * **************************************
             * testing fail-back 
             * 
             * **************************************
             */
            // Doing a fail back scenario to NN1
            cluster.restartNameNode(NN1); // will be restarted in the system with the next highest id while NN2 is still the leader
            cluster.waitActive();        
  
            waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN2), timeout * 10);
            
            cluster.shutdownNameNode(NN2);
            cluster.waitActive();
            
            // waiting for NN1 to elect itself as the leader
            waitLeaderElection(cluster.getDataNodes(), cluster.getNameNode(NN1), timeout * 10);
            assertTrue("NN1 is expected to be the leader, but is not", cluster.getNameNode(NN1).isLeader());
            assertTrue("Not all datanodes detected the new leader", doesDataNodesRecognizeLeader(cluster.getDataNodes(), cluster.getNameNode(NN1)));
        }
        catch (Exception ex)
        {
            LOG.error(ex);
            fail();
        }
        finally
        {
            if (cluster != null)
            {
                cluster.shutdown();
            }
        }

    }

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static boolean doesDataNodesRecognizeLeader(List<DataNode> datanodes, NameNode namenode)
    {
        boolean result = true;
        for (DataNode datanode : datanodes)
        {
            result = result & datanode.isLeader(namenode.getNameNodeAddress());
        }
        return result;
    }

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void waitLeaderElection(List<DataNode> datanodes, NameNode nn, long timeout) throws TimeoutException
    {
        // wait for the new leader to be elected
        long initTime = System.currentTimeMillis();
        while (!nn.isLeader())
        {

            try
            {
                Thread.sleep(2000); // 2sec
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }

            // check for time out
            if (System.currentTimeMillis() - initTime >= timeout)
            {
                throw new TimeoutException("Namenode was not elected leader");
            }
        }

        // wait for all datanodes to recognize the new leader
        initTime = System.currentTimeMillis();
        while (true)
        {

            try
            {
                Thread.sleep(2000); // 2sec
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }

            boolean result = doesDataNodesRecognizeLeader(datanodes, nn);
            if (result)
            {
                break;
            }
            // check for time out
            if (System.currentTimeMillis() - initTime >= timeout)
            {
                throw new TimeoutException("Datanodes weren't able to detect newly elected leader");
            }
        }
    }
}
