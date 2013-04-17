package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.Leader;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaderDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaderDerby extends LeaderDataAccess {

    private DerbyConnector connector = DerbyConnector.INSTANCE;

    @Override
    public int countAll() throws StorageException {
        try {
            Connection conn = connector.obtainSession();
            String query = String.format("select count(*) from %s", TABLE_NAME);
            PreparedStatement s;
            s = conn.prepareStatement(query);

            ResultSet result = s.executeQuery();
            return result.getInt(1);
        } catch (SQLException ex) {
            handleSQLException(ex);
            return 0;
        }
    }

    @Override
    public int countAllPredecessors(long id) throws StorageException {
        try {
            Connection conn = connector.obtainSession();
            String query = String.format("select count(*) from %s where %s<?" , TABLE_NAME, ID);
            PreparedStatement s;
            s = conn.prepareStatement(query);
            s.setLong(1, id);
            ResultSet result = s.executeQuery();
            return result.getInt(1);
        } catch (SQLException ex) {
            handleSQLException(ex);
            return 0;
        }
    }

    @Override
    public int countAllSuccessors(long id) throws StorageException{
        try {
            Connection conn = connector.obtainSession();
            String query = String.format("select count(*) from %s where %s>?" , TABLE_NAME, ID);
            PreparedStatement s;
            s = conn.prepareStatement(query);
            s.setLong(1, id);
            ResultSet result = s.executeQuery();
            return result.getInt(1);
        } catch (SQLException ex) {
            handleSQLException(ex);
            return 0;
        }
    }

//    @Override
//    public Leader findById(long id) throws StorageException {
//        try {
//            String query = String.format("select * from %s where %s=?", TABLE_NAME, ID);
//            Connection conn = connector.obtainSession();
//            PreparedStatement s = conn.prepareStatement(query);
//            s.setLong(1, id);
//            ResultSet rSet = s.executeQuery();
//            if (rSet.next()) {
//                return new Leader(rSet.getLong("ID"), rSet.getLong(COUNTER),
//                        rSet.getLong(TIMESTAMP), rSet.getString(HOSTNAME));
//            } else {
//                return null;
//            }
//        } catch (SQLException ex) {
//            handleSQLException(ex);
//            return null;
//        }
//    }

    @Override
    public Collection<Leader> findAllByCounterGT(long counter) throws StorageException {
        try {
            String query = String.format("select * from %s where %s > ?",
                    TABLE_NAME, COUNTER);
            Collection<Leader> results = null;
            Connection conn = connector.obtainSession();
            PreparedStatement s = conn.prepareStatement(query);
            s.setLong(1, counter);
            return convert(s.executeQuery());
        } catch (SQLException ex) {
            handleSQLException(ex);
            return Collections.EMPTY_LIST;
        }
    }

    @Override
    public Collection<Leader> findAll() throws StorageException {
        try {
            String query = String.format("select * from %s ", TABLE_NAME);
            Collection<Leader> results = null;
            Connection conn = connector.obtainSession();
            PreparedStatement s = conn.prepareStatement(query);
            return convert(s.executeQuery());
        } catch (SQLException ex) {
            handleSQLException(ex);
            return Collections.EMPTY_LIST;
        }
    }

    @Override
    public void prepare(Collection<Leader> removed, Collection<Leader> newed, Collection<Leader> modified) throws StorageException {
        String insert = String.format("insert into %s (%s,%s,%s,%s,%s,%s) values(?,?,?,?,?,?)",
                TABLE_NAME, ID, COUNTER, TIMESTAMP, HOSTNAME, AVG_REQUEST_PROCESSING_LATENCY, PARTITION_VAL);
        String update = String.format("update %s set %s=?, set %s=?, set %s=?, set %s=?, set %s=?, %s=? where %s=?",
                TABLE_NAME, ID, COUNTER, TIMESTAMP, HOSTNAME, AVG_REQUEST_PROCESSING_LATENCY, PARTITION_VAL);
        String delete = String.format("delete from %s where %s=?",
                TABLE_NAME, ID);
        Connection conn = connector.obtainSession();
        try {
            PreparedStatement updt = conn.prepareStatement(update);
            for (Leader l : modified) {
                updt.setLong(1, l.getId());
                updt.setLong(2, l.getCounter());
                updt.setLong(3, l.getTimeStamp());
                updt.setString(4, l.getHostName());
                updt.setInt(5, l.getAvgRequestProcessingLatency());
                updt.setInt(6, l.getPartitionVal());

                updt.addBatch();
            }
            updt.executeBatch();

            PreparedStatement insrt = conn.prepareStatement(insert);
            for (Leader l : newed) {
                insrt.setLong(1, l.getId());
                insrt.setLong(2, l.getCounter());
                insrt.setLong(3, l.getTimeStamp());
                insrt.setString(4, l.getHostName());
                insrt.setInt(5, l.getAvgRequestProcessingLatency());
                insrt.setInt(6, l.getPartitionVal());
                insrt.addBatch();
            }
            insrt.executeBatch();

            PreparedStatement dlt = conn.prepareStatement(delete);
            for (Leader l : removed) {
                dlt.setLong(1, l.getId());
                dlt.addBatch();
            }
            dlt.executeBatch();
        } catch (SQLException ex) {
            handleSQLException(ex);
        }
    }

    @Override
    public Collection<Leader> findAllByIDLT(long id) throws StorageException {
        try {
            String query = String.format("select * from %s where %s < ?",
                    TABLE_NAME, ID);
            Collection<Leader> results = null;
            Connection conn = connector.obtainSession();
            PreparedStatement s = conn.prepareStatement(query);
            s.setLong(1, id);
            return convert(s.executeQuery());
        } catch (SQLException ex) {
            handleSQLException(ex);
            return Collections.EMPTY_LIST;
        }
    }

    private Collection<Leader> convert(ResultSet rSet) throws SQLException {
        SortedSet<Leader> lSet = new TreeSet<Leader>();
        while (rSet.next()) {
            Leader leader = new Leader(rSet.getLong(ID), rSet.getLong(COUNTER),
                    rSet.getLong(TIMESTAMP), rSet.getString(HOSTNAME));
            lSet.add(leader);
        }
        return lSet;
    }

  @Override
  public Leader findByPkey(long id, int partitionKey) throws StorageException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
