package org.apache.hadoop.hdfs.server.namenode;


import se.sics.clusterj.*;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;

/**
 * This class provides the CRUD methods for Blocks and Triplets It also provides
 * helper methods for conversion to/from HDFS data structures to ClusterJ data
 * structures
 */
public class BlocksHelper {

    final static int RETRY_COUNT = 3;
    private static final int BLOCKTOTAL_ID = 1;

        /**
     * Return total blocks in BlockInfo table
     *
     * @param isTransactional - If its already part of a transaction (true) or
     * not (false)
     * @return total blocks
     * @throws ClusterJException
     */
    public static int getTotalBlocks() {
        Session session = DBConnector.obtainSession();
        int tries = RETRY_COUNT;
        boolean done = false;

        while (done == false && tries > 0) {
            try {
                int totalBlocks = (int) getTotalBlocksInternal(session);
                done = true;
                return totalBlocks;
            } catch (ClusterJException e) {
                System.err.println("getTotalBlocks failed " + e.getMessage());
                tries--;
            }
        }
        return 0;
    }

    /**
     * Reset the value of total blocks - Maybe required at startup or fresh
     * cluster setup
     */
    public static void resetTotalBlocks(boolean isTransactional) {
        Session session = DBConnector.obtainSession();
        Transaction tx = session.currentTransaction();

        assert tx.isActive() == isTransactional;

        if (isTransactional) {
            resetTotalBlocksInternal(session);
        } else {
            int tries = RETRY_COUNT;
            boolean done = false;

            while (done == false && tries > 0) {
                try {
                    DBConnector.beginTransaction();
                    resetTotalBlocksInternal(session);
                    DBConnector.commit();
                    session.flush();
                    done = true;
                } catch (ClusterJException e) {
                    DBConnector.safeRollback();
                    System.err.println("Reset of total blocks failed " + e.getMessage());
                    tries--;
                }
            }
        }
    }

    /////////////////////////////////////////////////////////////////////////////
    // Internal Functions
    /////////////////////////////////////////////////////////////////////////////
    /**
     * Reset the value of total blocks - Maybe required at startup or fresh
     * cluster setup
     */
    private static void resetTotalBlocksInternal(Session session) {
        BlockTotalTable blkTotal = session.newInstance(BlockTotalTable.class);
        blkTotal.setId(BLOCKTOTAL_ID);
        blkTotal.setTotal(0);
        session.makePersistent(blkTotal);
    }

    /**
     * Return total blocks in BlockInfo table
     *
     * @param isTransactional - If its already part of a transaction (true) or
     * not (false)
     * @throws ClusterJException
     */
    private static long getTotalBlocksInternal(Session session) throws ClusterJException {
        BlockTotalTable blkTable = session.find(BlockTotalTable.class, BLOCKTOTAL_ID);
        return blkTable.getTotal();
    }

}
