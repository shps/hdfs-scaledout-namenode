package org.apache.hadoop.hdfs.server.namenode.persistance;


import se.sics.clusterj.*;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;

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

  /** Return a BlockInfo object from an blockId 
   * @param blockId
   * @return
   * @throws IOException 
   */
//  public static BlockInfo getBlockInfo(long blockId) throws IOException {
//    int tries = RETRY_COUNT;
//    boolean done = false;
//
//    Session session = DBConnector.obtainSession();
//    while (done == false && tries > 0) {
//      try {
//        BlockInfo ret = getBlockInfo(session, blockId, false);
//        done = true;
//        return ret;
//      }
//      catch (ClusterJException e) {
//        //System.err.println("getBlockInfo failed " + e.getMessage());
//        e.printStackTrace();
//        tries--;
//      }
//
//    }
//    return null;
//  }  
  /** Return a Block object from an blockId 
   * @param blockId
   * @return
   * @throws IOException 
   */
//  public static Block getBlock(long blockId) {
//    int tries = RETRY_COUNT;
//    boolean done = false;
//
//    Session session = DBConnector.obtainSession();
//    while (done == false && tries > 0) {
//      try {
//        BlockInfoTable ret = selectBlockInfo(session, blockId);
//        Block b = new Block(ret.getBlockId(), ret.getNumBytes(), ret.getGenerationStamp());
//        done = true;
//        return b;
//      }
//      catch (ClusterJException e) {
//        //System.err.println("getBlockInfo failed " + e.getMessage());
//        e.printStackTrace();
//        tries--;
//      }
//
//    }
//    return null;
//  }  

  /** When called with single=false, will not retrieve the INodes for the Block */
//  private static BlockInfo getBlockInfo(Session session, long blockId, boolean single) throws IOException {
//    BlockInfoTable bit = selectBlockInfo(session, blockId);
//    return convert(bit);
//  }  
 /** Primary key lookup in the BlockInfo table using block ID
   * @param session
   * @param blkid
   * @return a row from the BlockInfo table
   */
//  private static BlockInfoTable selectBlockInfo(Session session, long blkid) {
//    return session.find(BlockInfoTable.class, blkid);
//  }
  /** When called with single=false, will not retrieve the INodes for the Block */
//  public static BlockInfo convert(BlockInfoTable bit) throws IOException {
//    return BlockInfoFactory.createBlockInfo(bit);
//  }
}
