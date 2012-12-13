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
import org.apache.hadoop.hdfs.server.namenode.Leader;

/**
 * This class provides the CRUD methods for [Leader] and [Counters] table
 */
public class LeaderHelper
{

    private static Log LOG = LogFactory.getLog(LeaderHelper.class);

    /*
     * Updates the running counter from [Counter]
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void updateCounter(long counter, long id, String hostname) throws IOException, PersistanceException
    {

        // update the counter in [Leader]
        Leader leader = new Leader(id, counter, now(), hostname);
        EntityManager.add(leader);
    }

    /**
     * Gets the running counter
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static long getCounter() throws IOException, PersistanceException
    {
        return EntityManager.count(Leader.Counter.AllById);
    }

    /**
     * Checks if the namenode exists in [LEADER]
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static boolean doesNamenodeExist(long leaderId) throws PersistanceException
    {
        
        Leader leader = EntityManager.find(Leader.Finder.ById, leaderId);
        
        if (leader == null)
        {
            return false;
        } else
        {
            return true;
        }
    }

    /**
     * Get max namenode id from [LEADER]
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static long getMaxNamenodeId() throws PersistanceException
    {
        List<Leader> namenodes = getAllNamenodesInternal();
        return getMaxNamenodeId(namenodes);
    }

    private static long getMaxNamenodeId(List<Leader> namenodes)
    {
        long maxId = 0;
        for (Leader lRecord : namenodes)
        {
            if (lRecord.getId() > maxId)
            {
                maxId = lRecord.getId();
            }
        }
        return maxId;
    }

    private static long getMinNamenodeId(List<Leader> namenodes)
    {
        long minId = Long.MAX_VALUE;
        for (Leader record : namenodes)
        {
            if (record.getId() < minId)
            {
                minId = record.getId();
            }
        }
        return minId;
    }

    /**
     * Gets the current potential leader - The namenode with the lowest id
     * returned is the eligble leader
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static long getLeader()
    {

        long maxCounter = EntityManager.Max;
        int totalNamenodes = getAllNamenodesInternal(session).size();
        if (totalNamenodes == 0)
        {
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
    public static SortedMap<Long, InetSocketAddress> getActiveNamenodes()
    {
        Session session = DBConnector.obtainSession();

        // get max counter and total namenode count
        long maxCounter = CountersHelper.getCounterValue(CountersHelper.LEADER_COUNTER_ID);
        int totalNamenodes = getAllNamenodesInternal(session).size();

        // get all active namenodes
        List<LeaderTable> nns = getActiveNamenodesInternal(session, maxCounter, totalNamenodes);

        // Order by id
        SortedMap<Long, InetSocketAddress> activennMap = new TreeMap<Long, InetSocketAddress>();
        for (LeaderTable l : nns)
        {
            InetSocketAddress addr = NetUtils.createSocketAddr(l.getHostname());
            activennMap.put(l.getId(), addr);
        }

        return activennMap;
    }

    /*
     * Remove previously selected leaders
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void removePrevoiouslyElectedLeaders(long id)
    {
        DBConnector.checkTransactionState(true);

        Session session = DBConnector.obtainSession();
        List<LeaderTable> prevLeaders = getPreceedingNamenodesInternal(session, id);
        for (LeaderTable l : prevLeaders)
        {
            deleteNamenodeInternal(session, l);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static int countPredecessors(long id)
    {
        Session session = DBConnector.obtainSession();
        return getPreceedingNamenodesInternal(session, id).size();
    }
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    public static int countSuccessors(long id)
    {
        Session session = DBConnector.obtainSession();
        return getSucceedingNamenodesInternal(session, id).size();
    }

    /*
     * Remove previously selected leaders
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void removeNamenode(long id)
    {
        DBConnector.checkTransactionState(true);

        Session session = DBConnector.obtainSession();
        LeaderTable record = getNamenodeInternal(session, id);
        deleteNamenodeInternal(session, record);
    }
    ///////////////////////////////////////////////////////////////////// 
    /////////////////// Internal functions/////////////////////////////
    ///////////////////////////////////////////////////////////////////// 

    private static void deleteNamenodeInternal(Session session, LeaderTable namenode)
    {
        session.deletePersistent(namenode);
    }

    private static LeaderTable getNamenodeInternal(Session session, long id)
    {
        return session.find(LeaderTable.class, id);
    }

    private static List<Leader> getAllNamenodesInternal() throws PersistanceException
    {
        List<Leader> leaders = (List<Leader>) EntityManager.findList(Leader.Finder.All);
        return leaders;
    }

    private static List<LeaderTable> getPreceedingNamenodesInternal(Session session, long id)
    {
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<LeaderTable> dobj = qb.createQueryDefinition(LeaderTable.class);
        Predicate pred = dobj.get("id").lessThan(dobj.param("id"));
        dobj.where(pred);
        Query<LeaderTable> query = session.createQuery(dobj);
        query.setParameter("id", id);
        return query.getResultList();
    }

    private static List<LeaderTable> getSucceedingNamenodesInternal(Session session, long id)
    {
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<LeaderTable> dobj = qb.createQueryDefinition(LeaderTable.class);
        Predicate pred = dobj.get("id").greaterThan(dobj.param("id"));
        dobj.where(pred);
        Query<LeaderTable> query = session.createQuery(dobj);
        query.setParameter("id", id);
        return query.getResultList();
    }

    private static List<LeaderTable> getActiveNamenodesInternal(Session session, long counter, int totalNamenodes)
    {
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<LeaderTable> dobj = qb.createQueryDefinition(LeaderTable.class);
        Predicate pred = dobj.get("counter").greaterThan(dobj.param("counter"));
        dobj.where(pred);
        Query<LeaderTable> query = session.createQuery(dobj);
        query.setParameter("counter", (counter - totalNamenodes));
        return query.getResultList();
    }

    private static void updateNamenodeInternal(Session session, LeaderTable namenode)
    {
        session.savePersistent(namenode);
        //session.makePersistent(namenode);
    }
}
