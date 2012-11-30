package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.persistance.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.LeaderHelper;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;

/**
 *
 * @author Jude
 */
public class LeaderElection extends Thread {

  private static final Log LOG = NameNode.LOG;
  public static final long LEADER_INITIALIZATION_ID = -1;
  // interval for monitoring leader
  private final long leadercheckInterval;
  // current Namenode where the leader election algorithm is running
  protected final NameNode nn;
  // current Leader NN
  protected long leaderId = -1;
  // list of actively running namenodes in the hdfs to be sent to DNs
  protected List<Long> nnList = new ArrayList<Long>();

  
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public LeaderElection(Configuration conf, NameNode nn) {
    this.nn = nn;
    this.leadercheckInterval = conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_KEY, DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_DEFAULT);
  }

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  protected void initialize() throws IOException {
      boolean isDone = false;
      try {
        int tries = DBConnector.RETRY_COUNT;

        while (!isDone && tries > 0) {
          try {
            isDone = false;
            DBConnector.beginTransaction();
            DBConnector.setExclusiveLock();
            // Determine the next leader and set it
            // if this is the leader, also remove previous leaders
            determineAndSetLeader();
            
            DBConnector.commit();
            isDone = true;
          }
          catch (ClusterJException ex) {
            if (!isDone) {
              DBConnector.safeRollback();
              tries--;
              LOG.error("LeaderMonitor.initialize() :: unable to perform leader election. Exception: " + ex.getMessage(), ex);
            } // end if
          } // end catch
        } // end while-retry
      } // try block for thread exceptions
      finally {
        if (!isDone) {
          DBConnector.safeRollback();
        }
      }

      // Start leader election thread
      start();
  }
  
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  protected void updateCounter() throws IOException {
    // retrieve counter in [COUNTER] table
    // Use EXCLUSIVE lock
    long counter = LeaderHelper.getCounter();
    
    // increment counter
    counter++;
    // Check to see if entry for this NN is in the [LEADER] table
    // May not exist if it was crashed and removed by another leader
    if(!LeaderHelper.doesNamenodeExist(nn.getId())) {
      nn.setId(LeaderHelper.getMaxNamenodeId()+1);
    }
    
    // store updated counter in [COUNTER] table
    // hostname is in "ip:port" format
    String hostname = nn.getNameNodeAddress().getAddress().getHostAddress()+":"+nn.getNameNodeAddress().getPort();
    //String hostname = nn.getNameNodeAddress().getAddress().getHostName()+":"+nn.getNameNodeAddress().getPort();
    //String hostname = hostname = nn.getNameNodeAddress().getAddress().getCanonicalHostName()+":"+nn.getNameNodeAddress().getPort();
    LeaderHelper.updateCounter(counter, nn.getId(), hostname);
  }

  /* The function that determines the current leader (or next potential leader) */
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  protected long select() {
    return LeaderHelper.getLeader();
  }

  /* The function that returns the list of actively running NNs */
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  SortedMap<Long, InetSocketAddress> selectAll() {
    return LeaderHelper.getActiveNamenodes();
  }

  /* Determines the leader */
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void determineAndSetLeader() {
      // Reset the leader (can be the same or new leader)
      leaderId = select();

      // If this node is the leader, remove all previous leaders
      if (leaderId == nn.getId() && !nn.isLeader() && leaderId != LEADER_INITIALIZATION_ID) {
        LOG.info("New leader elected. Namenode id: "+nn.getId() + ", rpcAddress: "+nn.getServiceRpcAddress() + ", Total Active namenodes in system: "+selectAll().size());
        // remove all previous leaders from [LEADER] table
        LeaderHelper.removePrevoiouslyElectedLeaders(leaderId);
        nn.setRole(NamenodeRole.LEADER);
      }
      
      // TODO [S] do something if i am no longer the leader. 
  }
  @Override
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void run() {
    while (nn.namesystem.isRunning()) {

      boolean isDone = false;
      try {
        int tries = DBConnector.RETRY_COUNT;

        while (!isDone && tries > 0) {
          try {
            isDone = false;
            DBConnector.beginTransaction();
            
            // Set the lock mode to exclusive so no other transactions can get access to this data
            DBConnector.setExclusiveLock();
            updateCounter();
            
            // Determine the next leader and set it
            // if this is the leader, also remove previous leaders
            determineAndSetLeader();
            
            DBConnector.commit();
            DBConnector.setDefaultLock();
            isDone = true;
          }
          catch (ClusterJException ex) {
            if (!isDone) {
              DBConnector.setDefaultLock();
              DBConnector.safeRollback();
              tries--;
              LOG.error("LeaderMonitor.run() :: unable to perform leader election. Exception: " + ex.getMessage(), ex);
            } // end if
          } // end catch
        } // end while-retry

        Thread.sleep(leadercheckInterval);
      } // try block for thread exceptions
      catch (InterruptedException ie) {
        LOG.warn("LeaderElection thread received InterruptedException.", ie);
        break;
      }
      catch (Throwable t) {
        LOG.warn("LeaderElection thread received Runtime exception. ", t);
        nn.stop();
        Runtime.getRuntime().exit(-1);
      }
      finally {
        if (!isDone) {
          DBConnector.setDefaultLock();
          DBConnector.safeRollback();
        }
      }
    } // main while loop
  }
}
