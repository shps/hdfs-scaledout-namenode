package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.net.NetUtils;

/**
 *
 * @author Jude
 * @author Salman Dec 2012. Removed helper classes
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
  private int missedHeartBeatThreshold = 1;

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public LeaderElection(Configuration conf, NameNode nn) {
    this.nn = nn;
    this.leadercheckInterval = conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_KEY, DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_DEFAULT);
    this.missedHeartBeatThreshold = conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD, DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
  }

  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  protected void initialize() {
    // Determine the next leader and set it
    // if this is the leader, also remove previous leaders
    try {
      new TransactionalRequestHandler(OperationType.LEADER_ELECTION) {

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          TransactionLockManager tlm = new TransactionLockManager();
          tlm.addLeaderLock(TransactionLockManager.LockType.READ_COMMITTED).
                  acquire();
        }

        @Override
        public Object performTask() throws PersistanceException, IOException {
          determineAndSetLeader();
          return null;
        }
      }.handle();

      // Start leader election thread
      start();

    } catch (Throwable t) {
      LOG.error("LeaderElection thread received Runtime exception. ", t);
      nn.stop();
      Runtime.getRuntime().exit(-1);
    }
  }

  /* Determines the leader */
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void determineAndSetLeader() throws IOException, PersistanceException {
    // Reset the leader (can be the same or new leader)
    leaderId = getLeader();

    // If this node is the leader, remove all previous leaders
    if (leaderId == nn.getId() && !nn.isLeader() && leaderId != LEADER_INITIALIZATION_ID) {
      LOG.info("New leader elected. Namenode id: " + nn.getId() + ", rpcAddress: " + nn.getServiceRpcAddress() + ", Total Active namenodes in system: " + selectAll().size());
      // remove all previous leaders from [LEADER] table
      removePrevoiouslyElectedLeaders(leaderId);
      nn.setRole(NamenodeRole.LEADER);
    }
    // TODO [S] do something if i am no longer the leader. 
  }
  private TransactionalRequestHandler leaderElectionHandler = new TransactionalRequestHandler(OperationType.LEADER_ELECTION) {

    @Override
    public void acquireLock() throws PersistanceException, IOException {
      TransactionLockManager tlm = new TransactionLockManager();
      tlm.addLeaderLock(TransactionLockManager.LockType.READ_COMMITTED).
              acquire();
    }

    @Override
    public Object performTask() throws PersistanceException, IOException {
      updateCounter();
      // Determine the next leader and set it
      // if this is the leader, also remove previous leaders
      determineAndSetLeader();
      return null;
    }
  };

  @Override
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void run() {
    // Leader election algorithm works in one transaction in each round. Reading all rows 
    // with read-committed lock should be fine. Because every NN only updates its row, and
    // the potential conflict could be when there are two leaders in the system try to delete
    // preceding rows which could end up in deadlock. 
    // we solved it by making sure that the namenodes delete the rows in the same order.
    
    while (nn.namesystem.isRunning()) {
      try {
        leaderElectionHandler.handle();
        Thread.sleep(leadercheckInterval);

      } catch (InterruptedException ie) {
        LOG.warn("LeaderElection thread received InterruptedException.", ie);
        break;
      } catch (Throwable t) {
        LOG.warn("LeaderElection thread received Runtime exception. ", t);
        nn.stop();
        Runtime.getRuntime().exit(-1);
      }
    } // main while loop
  }

  public long getLeader() throws IOException, PersistanceException {
    long maxCounter = getMaxNamenodeCounter();
    long totalNamenodes = getLeaderRowCount();
    if (totalNamenodes == 0) {
      LOG.warn("No namenodes in the system. The first one to start would be the leader");
      return LeaderElection.LEADER_INITIALIZATION_ID;
    }

    List<Leader> activeNamenodes = getActiveNamenodesInternal(maxCounter, totalNamenodes);
    return getMinNamenodeId(activeNamenodes);
  }

  protected void updateCounter() throws IOException, PersistanceException {
    // retrieve counter in [COUNTER] table
    // Use EXCLUSIVE lock
    long counter = getMaxNamenodeCounter();

    // increment counter
    counter++;
    // Check to see if entry for this NN is in the [LEADER] table
    // May not exist if it was crashed and removed by another leader
    if (!doesNamenodeExist(nn.getId())) {
      nn.setId(getMaxNamenodeId() + 1);
    }

    // store updated counter in [COUNTER] table
    // hostname is in "ip:port" format
    String hostname = nn.getNameNodeAddress().getAddress().getHostAddress() + ":" + nn.getNameNodeAddress().getPort();
    //String hostname = nn.getNameNodeAddress().getAddress().getHostName()+":"+nn.getNameNodeAddress().getPort();
    //String hostname = hostname = nn.getNameNodeAddress().getAddress().getCanonicalHostName()+":"+nn.getNameNodeAddress().getPort();
    updateCounter(counter, nn.getId(), hostname);
  }

  /* The function that returns the list of actively running NNs */
  //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  SortedMap<Long, InetSocketAddress> selectAll() throws IOException, PersistanceException {
    return getActiveNamenodes();
  }

  private long getMaxNamenodeCounter() throws PersistanceException {
    List<Leader> namenodes = getAllNamenodesInternal();
    return getMaxNamenodeCounter(namenodes);
  }

  private List<Leader> getAllNamenodesInternal() throws PersistanceException {

    List<Leader> leaders = (List<Leader>) EntityManager.findList(Leader.Finder.All);
    return leaders;
  }

  private long getMaxNamenodeCounter(List<Leader> namenodes) {
    long maxCounter = 0;
    for (Leader lRecord : namenodes) {
      if (lRecord.getCounter() > maxCounter) {
        maxCounter = lRecord.getCounter();
      }
    }
    return maxCounter;
  }

  private boolean doesNamenodeExist(long leaderId) throws PersistanceException {

    Leader leader = EntityManager.find(Leader.Finder.ById, leaderId);

    if (leader == null) {
      return false;
    } else {
      return true;
    }
  }

  public long getMaxNamenodeId() throws PersistanceException {
    List<Leader> namenodes = getAllNamenodesInternal();
    return getMaxNamenodeId(namenodes);
  }

  private static long getMaxNamenodeId(List<Leader> namenodes) {
    long maxId = 0;
    for (Leader lRecord : namenodes) {
      if (lRecord.getId() > maxId) {
        maxId = lRecord.getId();
      }
    }
    return maxId;
  }

  private void updateCounter(long counter, long id, String hostname) throws IOException, PersistanceException {
    // update the counter in [Leader]
    // insert the row. if it exists then update it
    // otherwise create a new row
    Leader leader = new Leader(id, counter, now(), hostname);
    EntityManager.add(leader);
  }

  private long getLeaderRowCount() throws IOException, PersistanceException {
    return EntityManager.count(Leader.Counter.AllById);
  }

  private List<Leader> getActiveNamenodesInternal(long counter, long totalNamenodes) throws PersistanceException {
    long condition = counter - totalNamenodes * missedHeartBeatThreshold;
    List<Leader> list = (List<Leader>) EntityManager.findList(Leader.Finder.AllByCounterGTN, condition);
    return list;
  }

  private static long getMinNamenodeId(List<Leader> namenodes) {
    long minId = Long.MAX_VALUE;
    for (Leader record : namenodes) {
      if (record.getId() < minId) {
        minId = record.getId();
      }
    }
    return minId;
  }

  public SortedMap<Long, InetSocketAddress> getActiveNamenodes() throws PersistanceException, IOException {
    // get max counter and total namenode count
    long maxCounter = getLeaderRowCount();
    int totalNamenodes = getAllNamenodesInternal().size();

    // get all active namenodes
    List<Leader> nns = getActiveNamenodesInternal(maxCounter, totalNamenodes);

    // Order by id
    SortedMap<Long, InetSocketAddress> activennMap = new TreeMap<Long, InetSocketAddress>();
    for (Leader l : nns) {
      InetSocketAddress addr = NetUtils.createSocketAddr(l.getHostName());
      activennMap.put(l.getId(), addr);
    }

    return activennMap;
  }

  public void removePrevoiouslyElectedLeaders(long id) throws PersistanceException {
    List<Leader> prevLeaders = getPreceedingNamenodesInternal(id);
    // Sort the leaders based on the id to avoid the scenario that there are two NNs
    // as leaders and both want to remove all preceding leaders which could result to
    // deadlock.
    Collections.sort(prevLeaders);
    for (Leader l : prevLeaders) {
      EntityManager.remove(l);
    }
  }

  private List<Leader> getPreceedingNamenodesInternal(long id) throws PersistanceException {
    List<Leader> list = (List<Leader>) EntityManager.findList(Leader.Finder.AllByIDLT, id);
    return list;
  }
}
