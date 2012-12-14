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
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.Leader;

/**
 * This class provides the CRUD methods for [Leader] and [Counters] table
 */
public class LeaderHelper
{

    private static Log LOG = LogFactory.getLog(xxx.class);

    /*
     * Updates the running counter from [Counter]
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void updateCounter(long counter, long id, String hostname) throws IOException, PersistanceException
    {
        // update the counter in [Leader]
        // insert the row. if it exists then update it
        // otherwise create a new row
        Leader leader = new Leader(id, counter, now(), hostname);
        EntityManager.add(leader);
    }

    /**
     * Gets the running counter
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static long getLeaderRowCount() throws IOException, PersistanceException
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

    public static long getMaxNamenodeCounter() throws PersistanceException
    {
        List<Leader> namenodes = getAllNamenodesInternal();
        return getMaxNamenodeCounter(namenodes);
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
    
    private static long getMaxNamenodeCounter(List<Leader> namenodes)
    {
        long maxCounter = 0;
        for (Leader lRecord : namenodes)
        {
            if (lRecord.getCounter() > maxCounter)
            {
                maxCounter = lRecord.getCounter();
            }
        }
        return maxCounter;
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
    public static long getLeader() throws PersistanceException, IOException 
    {
        long maxCounter = getMaxNamenodeCounter();
        long totalNamenodes = getLeaderRowCount();
        if (totalNamenodes == 0)
        {
            LOG.warn("No namenodes in the system. The first one to start would be the leader");
            return LeaderElection.LEADER_INITIALIZATION_ID;
        }

        List<Leader> activeNamenodes = getActiveNamenodesInternal(maxCounter, totalNamenodes);
        return getMinNamenodeId(activeNamenodes);
    }

    /**
     * Gets all currently running active namenodes
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static SortedMap<Long, InetSocketAddress> getActiveNamenodes() throws PersistanceException
    {
        // get max counter and total namenode count
        long maxCounter = CountersHelper.getCounterValue(CountersHelper.LEADER_COUNTER_ID);
        int totalNamenodes = getAllNamenodesInternal().size();

        // get all active namenodes
        List<Leader> nns = getActiveNamenodesInternal(maxCounter, totalNamenodes);

        // Order by id
        SortedMap<Long, InetSocketAddress> activennMap = new TreeMap<Long, InetSocketAddress>();
        for (Leader l : nns)
        {
            InetSocketAddress addr = NetUtils.createSocketAddr(l.getHostName());
            activennMap.put(l.getId(), addr);
        }

        return activennMap;
    }

    /*
     * Remove previously selected leaders
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void removePrevoiouslyElectedLeaders(long id) throws PersistanceException
    {
        List<Leader> prevLeaders = getPreceedingNamenodesInternal(id);
        for (Leader l : prevLeaders)
        {
            EntityManager.remove(l);
        }
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static int countPredecessors(long id) throws PersistanceException
    {
        return getPreceedingNamenodesInternal(id).size();
    }
   

    /*
     * Remove previously selected leaders
     */
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    public static void removeNamenode(long id) throws PersistanceException
    {
        Leader record = getNamenodeInternal(id);
        EntityManager.remove(record);
    }
    ///////////////////////////////////////////////////////////////////// 
    /////////////////// Internal functions/////////////////////////////
    ///////////////////////////////////////////////////////////////////// 
    private static Leader getNamenodeInternal(long id) throws PersistanceException
    {
        return EntityManager.find(Leader.Finder.ById, id);
    }

    private static List<Leader> getAllNamenodesInternal() throws PersistanceException
    {
        List<Leader> leaders = (List<Leader>) EntityManager.findList(Leader.Finder.All);
        return leaders;
    }

    private static List<Leader> getPreceedingNamenodesInternal(long id) throws PersistanceException
    {
        List<Leader> list = (List<Leader>) EntityManager.findList(Leader.Finder.AllByIDLT, id);
        return list;
    }

    private static List<Leader> getActiveNamenodesInternal(long counter, long totalNamenodes) throws PersistanceException
    {
        long condition = counter - totalNamenodes * Delta;
        List<Leader> list = (List<Leader>) EntityManager.findList(Leader.Finder.AllByCounterGTN, condition);
        return list;
    }
}
