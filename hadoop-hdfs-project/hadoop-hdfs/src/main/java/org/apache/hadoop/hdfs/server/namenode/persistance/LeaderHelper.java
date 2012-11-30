package org.apache.hadoop.hdfs.server.namenode.persistance;

import org.apache.hadoop.hdfs.server.namenode.persistance.CountersHelper;
import java.util.SortedMap;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.LeaderElection;
import org.apache.hadoop.net.NetUtils;
import se.sics.clusterj.LeaderTable;
import static org.apache.hadoop.hdfs.server.common.Util.now;

/** This class provides the CRUD methods for [Leader] and [Counters] table
 */
public class LeaderHelper {

  private static Log LOG = LogFactory.getLog(LeaderHelper.class);
  static final int RETRY_COUNT = 3;
  
  /*
   * Updates the running counter from [Counter]
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static void updateCounter(long value, long id, String hostname) throws IOException {
    DBConnector.checkTransactionState(true);
    
    Session session = DBConnector.obtainSession();
    // update the counter in [Counter]
    CountersHelper.updateCounter(CountersHelper.LEADER_COUNTER_ID, value);

    // update the counter in [Leader]
    LeaderTable l = session.newInstance(LeaderTable.class);
    l.setId(id);
    l.setCounter(value);
    l.setHostname(hostname);
    l.setTimestamp(now());    // default in ndb is now()
    updateNamenodeInternal(session, l);
    //TODO [S] why put new row every time. update the previous table
  }

  /**
   * Gets the running counter
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static long getCounter() throws IOException {
    return CountersHelper.getCounterValue(CountersHelper.LEADER_COUNTER_ID);
  }

  /**
   * Checks if the namenode exists in [LEADER]
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static boolean doesNamenodeExist(long leaderId) {
    Session session = DBConnector.obtainSession();
    if (getNamenodeInternal(session, leaderId) == null) {
      return false;
    }
    else {
      return true;
    }
  }

  /**
   * Get max namenode id from [LEADER]
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static long getMaxNamenodeId() {
    Session session = DBConnector.obtainSession();
    List<LeaderTable> namenodes = getAllNamenodesInternal(session);
    return getMaxNamenodeId(namenodes);
  }

  private static  long getMaxNamenodeId(List<LeaderTable> namenodes) {
    long maxId = 0;
    for (LeaderTable record : namenodes) {
      if (record.getId() > maxId) {
        maxId = record.getId();
      }
    }
    return maxId;
  }
  private static long getMinNamenodeId(List<LeaderTable> namenodes) {
    long minId = Long.MAX_VALUE;
    for (LeaderTable record : namenodes) {
      if (record.getId() < minId) {
        minId = record.getId();
      }
    }
    return minId;
  }

  /**
   * Gets the current potential leader - The namenode with the lowest id returned is the eligble leader
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static long getLeader() {
    Session session = DBConnector.obtainSession();
    
    long maxCounter = CountersHelper.getCounterValue(CountersHelper.LEADER_COUNTER_ID);
    int totalNamenodes = getAllNamenodesInternal(session).size();
    if(totalNamenodes == 0) {
      LOG.warn("No namenodes in the system. The first one to start would be the leader");
      return LeaderElection.LEADER_INITIALIZATION_ID;
    }
    
    List<LeaderTable> activeNamenodes = getActiveNamenodesInternal(session, maxCounter, totalNamenodes);
    return getMinNamenodeId(activeNamenodes);
  }
  
  /**
   * Gets all currently running active namenodes
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static SortedMap<Long, InetSocketAddress> getActiveNamenodes() {
    Session session = DBConnector.obtainSession();
    
    // get max counter and total namenode count
    long maxCounter = CountersHelper.getCounterValue(CountersHelper.LEADER_COUNTER_ID);
    int totalNamenodes = getAllNamenodesInternal(session).size();

    // get all active namenodes
    List<LeaderTable>  nns = getActiveNamenodesInternal(session, maxCounter, totalNamenodes);
    
    // Order by id
    SortedMap<Long, InetSocketAddress> activennMap = new TreeMap<Long, InetSocketAddress>();
    for(LeaderTable l : nns) {
      InetSocketAddress addr = NetUtils.createSocketAddr(l.getHostname());
      activennMap.put(l.getId(), addr);
    }
    
    return activennMap;
  }
  
  /*
   * Remove previously selected leaders
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static void removePrevoiouslyElectedLeaders(long id) {
    DBConnector.checkTransactionState(true);
    
    Session session = DBConnector.obtainSession();
    List<LeaderTable> prevLeaders = getPreceedingNamenodesInternal(session, id);
    for(LeaderTable l : prevLeaders) {
      deleteNamenodeInternal(session, l);
    }
  }

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static int countPredecessors(long id) {
    Session session = DBConnector.obtainSession();
    return getPreceedingNamenodesInternal(session, id).size();
  }
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static int countSuccessors(long id) {
    Session session = DBConnector.obtainSession();
    return getSucceedingNamenodesInternal(session, id).size();
  }

  /*
   * Remove previously selected leaders
   */
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static void removeNamenode(long id) {
    DBConnector.checkTransactionState(true);
    
    Session session = DBConnector.obtainSession();
    LeaderTable record = getNamenodeInternal(session, id);
    deleteNamenodeInternal(session, record);
  }
  ///////////////////////////////////////////////////////////////////// 
  /////////////////// Internal functions/////////////////////////////
  ///////////////////////////////////////////////////////////////////// 

  private static void deleteNamenodeInternal(Session session, LeaderTable namenode) {
    session.deletePersistent(namenode);
  }

  private static LeaderTable getNamenodeInternal(Session session, long id) {
    return session.find(LeaderTable.class, id);
  }

  private static List<LeaderTable> getAllNamenodesInternal(Session session) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaderTable> dobj = qb.createQueryDefinition(LeaderTable.class);
    Query<LeaderTable> query = session.createQuery(dobj);
    return query.getResultList();
  }

  private static List<LeaderTable> getPreceedingNamenodesInternal(Session session, long id) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaderTable> dobj = qb.createQueryDefinition(LeaderTable.class);
    Predicate pred = dobj.get("id").lessThan(dobj.param("id"));
    dobj.where(pred);
    Query<LeaderTable> query = session.createQuery(dobj);
    query.setParameter("id", id);
    return query.getResultList();
  }
  private static List<LeaderTable> getSucceedingNamenodesInternal(Session session, long id) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaderTable> dobj = qb.createQueryDefinition(LeaderTable.class);
    Predicate pred = dobj.get("id").greaterThan(dobj.param("id"));
    dobj.where(pred);
    Query<LeaderTable> query = session.createQuery(dobj);
    query.setParameter("id", id);
    return query.getResultList();
  }

  private static List<LeaderTable> getActiveNamenodesInternal(Session session, long counter, int totalNamenodes) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<LeaderTable> dobj = qb.createQueryDefinition(LeaderTable.class);
    Predicate pred = dobj.get("counter").greaterThan(dobj.param("counter"));
    dobj.where(pred);
    Query<LeaderTable> query = session.createQuery(dobj);
    query.setParameter("counter", (counter - totalNamenodes));
    return query.getResultList();
  }

  private static void updateNamenodeInternal(Session session, LeaderTable namenode) {
    session.savePersistent(namenode);
    //session.makePersistent(namenode);
  }
}
