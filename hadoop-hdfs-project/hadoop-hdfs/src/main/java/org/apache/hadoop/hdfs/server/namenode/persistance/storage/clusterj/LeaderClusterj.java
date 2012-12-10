package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.Leader;

import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaderDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class LeaderClusterj extends LeaderDataAccess
{

    @PersistenceCapable(table = TABLE_NAME)
    public interface LeaderDTO
    {

        @PrimaryKey
        @Column(name = ID)
        long getId();

        void setId(long id);

        @PrimaryKey
        @Column(name = PARTITION_VAL)
        int getPartitionVal();

        void setPartitionVal(int partitionVal);

        @Column(name = COUNTER)
        long getCounter();

        void setCounter(long counter);

        @Column(name = TIMESTAMP)
        long getTimestamp();

        void setTimestamp(long timestamp);

        @Column(name = HOSTNAME)
        String getHostname();

        void setHostname(String hostname);

        @Column(name = AVG_REQUEST_PROCESSING_LATENCY)
        int getAvgRequestProcessingLatency();

        void setAvgRequestProcessingLatency(int avgRequestProcessingLatency);
    }
    private ClusterjConnector connector = ClusterjConnector.INSTANCE;

    @Override
    public int countAllById() throws StorageException
    {
        return findAll().size();
    }

    @Override
    public Leader findById(long id) throws StorageException
    {
         try
        {
            Session session = connector.obtainSession();
            LeaderDTO lTable = session.find(LeaderDTO.class, id);
            if (lTable != null)
            {
                Leader leader = createLeader(lTable);
                return leader;
            }
            return null;
        } catch (Exception e)
        {
            throw new StorageException(e);
        }
    }

    @Override
    public Collection<Leader> findAllByCounter(long counter) throws StorageException
    {
        // FIXME
        // start fromthere
        
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<Leader> findAll() throws StorageException
    {
        try
        {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<LeaderDTO> dobj = qb.createQueryDefinition(LeaderDTO.class);
            Query<LeaderDTO> query = session.createQuery(dobj);
            return createList(query.getResultList());
        } catch (Exception e)
        {
            throw new StorageException(e);
        }
    }

    @Override
    public void prepare(Collection<Leader> removed, Collection<Leader> newed, Collection<Leader> modified) throws StorageException
    {
        try
        {
            Session session = connector.obtainSession();
            for (Leader l : newed)
            {
                LeaderDTO lTable = session.newInstance(LeaderDTO.class);
                createPersistableLeaderInstance(l, lTable);
                session.savePersistent(lTable);
            }

            for (Leader l : modified)
            {
                LeaderDTO lTable = session.newInstance(LeaderDTO.class);
                createPersistableLeaderInstance(l, lTable);
                session.savePersistent(lTable);
            }

            for (Leader l : removed)
            {
                Object[] pk = new Object[2];
                pk[0] = l.getId();
                pk[1] = l.getPartitionVal();
                LeaderDTO lTable = session.newInstance(LeaderDTO.class, pk);
                session.deletePersistent(lTable);
            }
        } catch (Exception e)
        {
            throw new StorageException(e);
        }
    }

///
    @Override
    public int countAll() throws StorageException
    {
        return findAll().size();
    }


    @Override
    public Lease findByHolderId(int holderId) throws StorageException
    {
        try
        {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<LeaseDTO> dobj = qb.createQueryDefinition(LeaseDTO.class);

            dobj.where(dobj.get("holderId").equal(dobj.param("param")));

            Query<LeaseDTO> query = session.createQuery(dobj);
            query.setParameter("param", holderId); //the WHERE clause of SQL
            List<LeaseDTO> leaseTables = query.getResultList();

            if (leaseTables.size() > 1)
            {
                LeaseManager.LOG.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
                return null;
            } else if (leaseTables.size() == 1)
            {
                Lease lease = createLease(leaseTables.get(0));
                return lease;
            } else
            {
                LeaseManager.LOG.info("No rows found for holderID:" + holderId + " in Lease table");
                return null;
            }
        } catch (Exception e)
        {
            throw new StorageException(e);
        }
    }

    @Override
    public Collection<Lease> findByTimeLimit(long timeLimit) throws StorageException
    {
        try
        {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType dobj = qb.createQueryDefinition(LeaseDTO.class);
            PredicateOperand propertyPredicate = dobj.get("lastUpdate");
            String param = "timelimit";
            PredicateOperand propertyLimit = dobj.param(param);
            Predicate lessThan = propertyPredicate.lessThan(propertyLimit);
            dobj.where(lessThan);
            Query query = session.createQuery(dobj);
            query.setParameter(param, new Long(timeLimit));
            return createList(query.getResultList());
        } catch (Exception e)
        {
            throw new StorageException(e);
        }
    }

    private SortedSet<Leader> createList(List<LeaderDTO> list)
    {
        SortedSet<Leader> finalSet = new TreeSet<Leader>();
        for (LeaderDTO dto : list)
        {
            finalSet.add(createLeader(dto));
        }

        return finalSet;
    }

    private Leader createLeader(LeaderDTO lTable)
    {
        return new Leader(lTable.getId(),
                lTable.getCounter(),
                lTable.getTimestamp(),
                lTable.getHostname(),
                lTable.getAvgRequestProcessingLatency(),
                lTable.getPartitionVal());
    }

    private void createPersistableLeaderInstance(Leader leader, LeaderDTO lTable)
    {
        lTable.setId(leader.getId());
        lTable.setCounter(leader.getCounter());
        lTable.setHostname(leader.getHostName());
        lTable.setTimestamp(leader.getTimeStamp());
        lTable.setAvgRequestProcessingLatency(leader.getAvg_request_processing_latency());
        lTable.setPartitionVal(leader.getPartition_val());
    }
}
