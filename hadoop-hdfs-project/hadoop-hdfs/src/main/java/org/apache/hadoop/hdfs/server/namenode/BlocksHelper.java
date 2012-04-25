package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;

import se.sics.clusterj.*;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;

/** This class provides the CRUD methods for Blocks and Triplets
 *  It also provides helper methods for conversion to/from HDFS data structures to ClusterJ data structures
 */
public class BlocksHelper {

	private static Log LOG = LogFactory.getLog(BlocksHelper.class);
	private static DatanodeManager dnm = null;
	static final int RETRY_COUNT = 3; 
	private final static int BLOCKTOTAL_ID = 1;

	/**Sets the FSNamesystem object. This method should be called before using any of the helper functions in this class.
	 * @param fsns an already initialized FSNamesystem object
	 */
	public static void initialize(DatanodeManager dnm) {
		BlocksHelper.dnm = dnm;
	}

	/**
	 * Helper function for appending an array of blocks - used by concat
	 * Replacement for INodeFile.appendBlocks
	 */
	public static void appendBlocks(INodeFile [] inodes, int totalAddedBlocks) {
		int tries=RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0 ){
			try{	
				tx.begin();
				appendBlocks(inodes, totalAddedBlocks, session);
				tx.commit();
				done=true;
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				//System.err.println("BlocksHelper.appendBlocks() threw error " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}

	}
	private static void appendBlocks(INodeFile[] inodes, int totalAddedBlocks, Session session){
		for(INodeFile in: inodes) {
			BlockInfo[] inBlocks = in.getBlocks();
			for(int i=0;i<inBlocks.length;i++) {
				BlockInfoTable bInfoTable = createBlockInfoTable(inBlocks[i], session);
				insertBlockInfo(session, bInfoTable);
			}
		}
	}

	/**
	 * Helper function for inserting a block in the BlocksInfo table
	 * Replacement for INodeFile.addBlock
	 * */
	public static void addBlock(BlockInfo newblock, boolean isTransactional) {
		putBlockInfo(newblock, isTransactional);

		// ADDED: To keep update of total blocks in the system
		updateTotalBlocks(true);
	}
	/**
	 * Updates total blocks in block table
	 * @param session
	 * @param toAdd - If true, we increment total blocks else we decrement
	 */
	private static void updateTotalBlocks(boolean toAdd)
	{
		Session session = DBConnector.obtainSession();
		BlockTotalTable blkTotal = session.find(BlockTotalTable.class, BLOCKTOTAL_ID);
		if(toAdd == true)
		{
			blkTotal.setTotal(blkTotal.getTotal() + 1);
		}
		else
		{
			blkTotal.setTotal(blkTotal.getTotal() - 1);
		}
		updateTotalBlocksInternal(session, blkTotal);
	}


	/**Helper function for creating a BlockInfoTable object, no DB access */
	private static BlockInfoTable createBlockInfoTable(BlockInfo newblock, Session session) {
		BlockInfoTable bInfoTable = session.newInstance(BlockInfoTable.class);
		bInfoTable.setBlockId(newblock.getBlockId());
		bInfoTable.setGenerationStamp(newblock.getGenerationStamp());
		bInfoTable.setINodeID(newblock.getINode().getID()); 
		bInfoTable.setNumBytes(newblock.getNumBytes());
		bInfoTable.setReplication(-1); //FIXME: see if we need to store this or not
		return bInfoTable;
	}


	/** Return a BlockInfo object from an blockId 
	 * @param blockId
	 * @return
	 * @throws IOException 
	 */
	public static BlockInfo getBlockInfo(long blockId)  {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				BlockInfo ret = getBlockInfo(session, blockId, false);
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				//System.err.println("getBlockInfo failed " + e.getMessage());
				e.printStackTrace();	
				tries--;
			}

		}
		return null;
	}
	/** When called with single=false, will not retrieve the INodes for the Block */
	private static BlockInfo getBlockInfo(Session session, long blockId, boolean single) {
		BlockInfoTable bit = selectBlockInfo(session, blockId);
		if(bit == null)
		{
			return null;
		}
		else {
			Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());
			BlockInfo blockInfo = new BlockInfo(b, bit.getReplication());

			if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMMITTED.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.COMMITTED);
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE.ordinal())
			{
				blockInfo = new BlockInfo(b, bit.getReplication());
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION);
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_RECOVERY.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.UNDER_RECOVERY);
			}

			//W: assuming that this function will only be called on an INodeFile
			if (single == false){ 
				INodeFile node = (INodeFile)INodeHelper.getINode(bit.getINodeID());
				if (node != null) { 
					node.setBlocksList(getBlocksArrayInternal(node, session));

					blockInfo.setINodeWithoutTransaction(node);
					updateBlockInfoTable(node.getID(), blockInfo, session);
				}
			}
			blockInfo.setBlockIndex(bit.getBlockIndex()); 
			blockInfo.setTimestamp(bit.getTimestamp());

			return blockInfo;
		}

	}
	/** Returns a BlockInfo object without any Inodes set for it (single=true) */
	public static BlockInfo getBlockInfoSingle(long blockId) throws IOException {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				BlockInfo ret = getBlockInfo(session, blockId, true);
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				//System.err.println("getBlockInfoSingle failed " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		return null;
	}

        /**
         * 
         * @param binfo 
         */
        public static void putBlockInfo(BlockInfo binfo, boolean isTransactional) {
            DBConnector.checkTransactionState(isTransactional);
            if (isTransactional)
                putBlockInfo(binfo, DBConnector.obtainSession());
            else
                putBlockInfoWithTransaction(binfo);
	}
        
	private static void putBlockInfoWithTransaction(BlockInfo binfo) {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				putBlockInfo(binfo, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
                                LOG.error(e.getMessage(), e);
                                if (tx.isActive())
                                    tx.rollback();
				tries--;
			}
		}
	}
	
        private static void putBlockInfo(BlockInfo binfo, Session s){
		
		BlockInfoTable bit = selectBlockInfo(s, binfo.getBlockId());
		
		if(bit == null) { //we want to generate a timestamp only if it doesnt exist in NDB
			bit =  s.newInstance(BlockInfoTable.class);
			bit.setTimestamp(System.currentTimeMillis());
		}
		
		bit.setBlockId(binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());

		if(binfo.isComplete()) {
			INodeFile ifile = binfo.getINode();
			long nodeID = ifile.getID();
			bit.setINodeID(nodeID); 
		}
		bit.setNumBytes(binfo.getNumBytes());
		insertBlockInfo(s, bit);
	}


        /** Update index for a BlockInfo object in the DB
	 * @param idx index of the BlockInfo
	 * @param binfo BlockInfo object that already exists in the database
	 */
	public static void updateIndex(int idx, BlockInfo binfo, boolean isTransactional) {
            Session session = DBConnector.obtainSession();
            boolean isActive = session.currentTransaction().isActive();
            assert isActive == isTransactional : 
                    "Current transaction's isActive value is " + isActive + 
                    " but isTransactional's value is " + isTransactional;
            
            if (isTransactional)
                updateIndex(idx, binfo, session);
            else
                updateIndexWithTransaction(idx, binfo);
        }
        
	/** Update index for a BlockInfo object in the DB. Begins a new transaction.
	 * @param idx index of the BlockInfo
	 * @param binfo BlockInfo object that already exists in the database
	 */
	private static void updateIndexWithTransaction(int idx, BlockInfo binfo) {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
                
                while (done == false && tries > 0) {
                    try {
                            tx.begin();
                            updateIndex(idx, binfo, session);
                            tx.commit();
                            session.flush();
                            done=true;
                    }
                    catch (ClusterJException e){
                            tx.rollback();
                            System.err.println("updateIndex failed " + e.getMessage());
                            tries--;
                        }
                }
	}
        
	private static void updateIndex(int idx, BlockInfo binfo, Session s){
		LOG.debug("Block persistance: ID:" + binfo.getBlockId() + "    index:" + idx + ", status:" + binfo.getBlockUCState() );
		BlockInfoTable bit =  s.newInstance(BlockInfoTable.class);
		bit.setBlockId(binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		bit.setINodeID(binfo.getINode().getID());
		bit.setBlockIndex(idx); //setting the index in the table
		LOG.debug("W: blockId"+binfo.getBlockId()+ " Index updated to" + idx );
		bit.setNumBytes(binfo.getNumBytes());
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());
		updateBlockInfoTableInternal(s, bit);

	}



	/** Updates an already existing BlockInfo object in DB
	 * @param iNodeID
	 * @param binfo
	 */
	public static void updateBlockInfoInDB(long iNodeID, BlockInfo binfo, boolean isTransactional){
            DBConnector.checkTransactionState(isTransactional);
            
            if (isTransactional)
                updateBlockInfoTable(iNodeID, binfo, DBConnector.obtainSession());
            else
                updateBlockInfoInDBWithTransaction(iNodeID, binfo);
        }
	
	/** Updates an already existing BlockInfo object in DB. Begins a new transaction.
	 * @param iNodeID
	 * @param binfo
	 */
	private static void updateBlockInfoInDBWithTransaction(long iNodeID, BlockInfo binfo){ 
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while (done == false && tries > 0) {
			try {
				tx.begin();
				updateBlockInfoTable(iNodeID, binfo, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
                                LOG.error(e.getMessage(), e);
                                if (tx.isActive())
                                    tx.rollback();
				tries--;
			}
		}
	}


	private static void updateBlockInfoTable(long iNodeID, BlockInfo binfo, Session s) {
		BlockInfoTable bit =  s.newInstance(BlockInfoTable.class, binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		//setting the iNodeID here - the rest remains the same
		bit.setINodeID(iNodeID); 
		bit.setNumBytes(binfo.getNumBytes());
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());
		updateBlockInfoTableInternal(s, bit);
	}

	public static List<TripletsTable> selectTripletsUsingDatanodeAndBlockID(String hostNameValue, long nextValue){ 
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				List <TripletsTable> ret = selectTriplets(
						hostNameValue, nextValue, 
						session);
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				//System.err.println("updateIndex failed " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		return null;
	}

	public static BlockInfo[] getBlockInfoArray(INodeFile inode) {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				BlockInfo[] ret = getBlocksArrayInternal(inode, session);
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				//System.err.println("getBlocksArray failed " + e.getMessage());
				e.printStackTrace();
				tries--;
			}

		}
		return null;
	}

	public static BlockInfo[] getBlocksArrayInternal(INodeFile inode, Session session) {
		if(inode==null)
			return null;

		List<BlockInfoTable> blocksList = selectAllBlockInfo(session, inode.getID());

		if(blocksList.size() == 0 || blocksList == null) {
			return null;
		}

		BlockInfo[] blocksArray = new BlockInfo[blocksList.size()];

		for(int i=0; i<blocksArray.length; i++) {
			// Now we're effectively calling getBlockInfoSingle()
			blocksArray[i] = getBlockInfo(session, blocksList.get(i).getBlockId(), true);
			blocksArray[i].setINodeWithoutTransaction(inode);
			updateBlockInfoTable(inode.getID(), blocksArray[i], session);
		}
		//sorting the array in descending order w.r.t blockIndex
		Arrays.sort(blocksArray, new BlockInfoComparator());
		return blocksArray;
	}

	/**
	 * A Comparator to sort BlockInfo according to timestamp
	 *
	 */
	public static class BlockInfoComparator implements Comparator<BlockInfo> {

		/*
		@Override
		public int compare(BlockInfo o1, BlockInfo o2) {
			return o1.getTimestamp() < o2.getTimestamp() ? -1 : 1;
		}
		 * 
		 */
		@Override
		/* For this to work, ensure that by default value for column BlockIndex in table [BlockInfo] is -1 */
		public int compare(BlockInfo o1, BlockInfo o2) {
			return o1.getTimestamp() < o2.getTimestamp() ? -1 : 1;
		}

	}

        /**
         * Update the DataNode in the triplets table.
         * @param blockId
         * @param index
         * @param name 
         */
	public static void setDatanode(long blockId, int index, String name, boolean isTransactional) {
            Session session = DBConnector.obtainSession();
            boolean isActive = session.currentTransaction().isActive();
            assert isActive == isTransactional : 
                    "Current transaction's isActive value is " + isActive + 
                    " but isTransactional's value is " + isTransactional;
            
            if (isTransactional)
                setDatanodeInternal(blockId, index, name, session);
            else
                setDatanodeWithTransaction(blockId, index, name);
        }
        
	/** Update the DataNode in the triplets table. This begins a new transaction.*/
	private static void setDatanodeWithTransaction(long blockId, int index, String name) {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				setDatanodeInternal(blockId, index, name, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
                                LOG.error(e.getMessage(), e);
                                if (tx.isActive())
                                    tx.rollback();
				tries--;
			}
		}
	}	

	private  static void setDatanodeInternal(long blockId, int index, String name, Session session) {

		TripletsTable triplet = selectTriplet(session, blockId, index);
		//TODO: [thesis] this update can be done in one shot TODO TODO TODO
		if (triplet != null) {
			LOG.debug("WASIF triplet exists for blkid:"+blockId + "index:" + index);
			triplet.setDatanodeName(name);
			triplet.setIndex(index);
			insertTriplet(session, triplet, true);
		}
		else {
			LOG.debug("WASIF new triplet being created for blkid:"+blockId + "index:" + index);
			TripletsTable newTriplet = session.newInstance(TripletsTable.class);
			newTriplet.setBlockId(blockId);
			newTriplet.setDatanodeName(name);
			newTriplet.setIndex(index);
			insertTriplet(session, newTriplet, true); //TODO: this should be false rite?
		}
	}

	public static DatanodeDescriptor getDatanode (long blockId, int index){
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				DatanodeDescriptor ret = getDataDescriptorInternal(blockId, index, session);
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				//System.err.println("setDataNode failed " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		return null;
	}	

	private static DatanodeDescriptor getDataDescriptorInternal(long blockId, int index, Session session){
		TripletsTable triplet = selectTriplet(session, blockId, index);

		if (triplet != null && triplet.getDatanodeName() != null) {
			DatanodeDescriptor ret = dnm.getDatanodeByName(triplet.getDatanodeName());
			return ret;
		}
		return null;
	}

	public static DatanodeDescriptor[] getDataNodesFromBlock (long blockId){
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				DatanodeDescriptor[] ret = getDataNodesFromBlockInternal(blockId, session);
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				//System.err.println("getDataNodesFromBlock failed " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		return null;
	}


	private static DatanodeDescriptor[] getDataNodesFromBlockInternal (long blockId, Session session){
		List<TripletsTable> result = selectTriplet(session, blockId);
		DatanodeDescriptor[] nodeDescriptor = new DatanodeDescriptor[result.size()];
		int i = 0;
		for (TripletsTable t: result){
			DatanodeID dn = new DatanodeID(t.getDatanodeName());
			nodeDescriptor[i] = new DatanodeDescriptor(dn);
			i++;
		}
		return nodeDescriptor;
	}


	private static List<TripletsTable> getTripletsListUsingFieldInternal(String field, long value, Session s){
		QueryBuilder qb = s.getQueryBuilder();
		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
		dobj.where(dobj.get(field).equal(dobj.param("param")));
		Query<TripletsTable> query = s.createQuery(dobj);
		query.setParameter("param", value);
		return query.getResultList();

	}

	private static List<TripletsTable> getTripletsListUsingFieldInternal(String field, String value, Session s){
		QueryBuilder qb = s.getQueryBuilder();
		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
		dobj.where(dobj.get(field).equal(dobj.param("param")));
		Query<TripletsTable> query = s.createQuery(dobj);
		query.setParameter("param", value);
		return query.getResultList();
	}

        public static BlockInfo removeBlocks(Block key, boolean isTransactional){
		DBConnector.checkTransactionState(isTransactional);
                BlockInfo ret = null;
                
                if (isTransactional)
                {
                    Session session = DBConnector.obtainSession();
                    ret = removeBlocksInternal(key, session);
					updateTotalBlocks( false);
                    session.flush();
                }
                else
                    ret = removeBlocksWithTransaction(key);
                
                return ret;
	}
        
	public static BlockInfo removeBlocksWithTransaction(Block key){
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				BlockInfo ret = removeBlocksInternal(key, session);
				updateTotalBlocks( false);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				if(tx.isActive())
					tx.rollback();
				LOG.error(e.getMessage(), e);
				tries--;
			}
		}
		return null;
	}


	private static BlockInfo removeBlocksInternal(Block key, Session s) 
	{
		long blockId = key.getBlockId();
		BlockInfo bi = new BlockInfo(1);
		bi.setBlockId(blockId);
		deleteBlockInfo(s, blockId);
		return bi;
	}

	private static void removeTripletsInternal(Session session, BlockInfo blockInfo, int index)
	{
		deleteTriplet(session, blockInfo.getBlockId(), index);
		// The triplets entries in the DB for a block have an ordered list of
		// indices. Removal of an entry of an index X means that all entries
		// with index greater than X needs to be corrected (decremented by 1
		// basically).
		//TODO: wowowowo!!! lots of stuff happening here, later!
		List<TripletsTable> results = selectTriplet(session, blockInfo.getBlockId());
		Collections.sort(results, new TripletsTableComparator());
		for (TripletsTable t: results)	{
			long oldIndex = t.getIndex();

			// entry that needs its index corrected
			if (index < oldIndex)
			{				
				TripletsTable replacementEntry = session.newInstance(TripletsTable.class);
				replacementEntry.setBlockId(t.getBlockId());
				replacementEntry.setDatanodeName(t.getDatanodeName());
				replacementEntry.setIndex(t.getIndex() - 1); // Correct the index
				deleteTriplet(session, t.getBlockId(), t.getIndex());
				insertTriplet(session, replacementEntry, false);
			}
		}
	}
        
        /**
         * 
         * @param blockInfo
         * @param index 
         */
	public static void removeTriplets(BlockInfo blockInfo, int index,
               boolean isTransactional)
	{ 
            DBConnector.checkTransactionState(isTransactional);
            
            if (isTransactional)
            {
                Session session = DBConnector.obtainSession();
                removeTripletesInternal(blockInfo, index, 
                        session);
                session.flush();
            }
            else
                removeTripletsWithTransaction(blockInfo, index);
        }
        /**
         * 
         * @param blockInfo
         * @param index 
         */
	private static void removeTripletsWithTransaction(BlockInfo blockInfo, int index)
	{ 
            int tries = RETRY_COUNT;
            boolean done = false;

            Session session = DBConnector.obtainSession();
            Transaction tx = session.currentTransaction();

            while (done == false && tries > 0) {
                try {
                    tx.begin();
                    removeTripletesInternal(blockInfo, index, session);
                    tx.commit();
                    session.flush();
                    done = true;
                } catch(ClusterJException e)
                {
                    LOG.error(e.getMessage(), e);
                    if (tx.isActive())
                        tx.rollback();
                    tries--;
                }
            }
	}

        private static void removeTripletesInternal(BlockInfo blockInfo, int index,
                Session session)
        {
            deleteTriplet(session, blockInfo.getBlockId(), index);
                    // The triplets entries in the DB for a block have an ordered list of
                    // indices. Removal of an entry of an index X means that all entries
                    // with index greater than X needs to be corrected (decremented by 1
                    // basically).
                    //TODO: wowowowo!!! lots of stuff happening here, later!
                    List<TripletsTable> results = selectTriplet(session, blockInfo.getBlockId());
                    Collections.sort(results, new TripletsTableComparator());
                    for (TripletsTable t: results)	{
                            long oldIndex = t.getIndex();

                            // entry that needs its index corrected
                            if (index < oldIndex)
                            {				
                                    TripletsTable replacementEntry = session.newInstance(TripletsTable.class);
                                    replacementEntry.setBlockId(t.getBlockId());
                                    replacementEntry.setDatanodeName(t.getDatanodeName());
                                    replacementEntry.setIndex(t.getIndex() - 1); // Correct the index
                                    deleteTriplet(session, t.getBlockId(), t.getIndex());
                                    insertTriplet(session, replacementEntry, false);
                            }
                    }
        }
        
	/**
	 * A Comparator to sort the Triplets according to index
	 *
	 */
	private static class TripletsTableComparator implements Comparator<TripletsTable> {
		@Override
		public int compare(TripletsTable o1, TripletsTable o2) {
			return o1.getIndex() < o2.getIndex() ? -1 : 1;
		}
	}

	/** Given a BlockInfo object, find the number of triplets that exist  */
	public static int getTripletsForBlockLength (BlockInfo blockinfo) {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				int ret = selectTriplet(session, blockinfo.getBlockId()).size();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
                LOG.error(e.getMessage(), e);
				tries--;
			}
		}
		return -1;
	}


	private static int getTripletsForBlockLengthInternal (BlockInfo blockinfo, Session session) {//FIXME: [thesis]
		List<TripletsTable> results = getTripletsListUsingFieldInternal ("blockId", blockinfo.getBlockId(), session);
		return results.size();
	}


	public static INode getInodeFromBlockId (long blockId) {
		Session session = DBConnector.obtainSession();
		BlockInfoTable blockInfoTable = selectBlockInfo(session, blockId);
		return INodeHelper.getINode(blockInfoTable.getINodeID());
	}

	public static int getTripletsIndex(DatanodeDescriptor node, long blockId) 
	{
		int blockIndex;
		int tries = RETRY_COUNT;
		boolean done = false;
		while (done == false && tries > 0) {
			try {
				List<TripletsTable> triplets = selectTripletsUsingDatanodeAndBlockID(node.getName(), blockId);
				if(triplets!=null && triplets.size()==1)
				{	
					blockIndex = triplets.get(0).getIndex();
					done=true;
					return blockIndex;
				}
				else
					return -1;
			}
			catch (ClusterJException e){
				//System.err.println("getTripletsIndex failed " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
			finally {
			}
		}
		return -1;
	}

	/*
	 * This replaces BlockInfo.findDatanode().
	 * It finds the rows corresponding to a (blockId, datanode) tuple,
	 * and returns the lowest value of index among the
	 * returned results. 
	 */
	public static int findDatanodeForBlock(DatanodeDescriptor node, long blockId) 
	{
		Session session = DBConnector.obtainSession();
		List<TripletsTable> results = selectTriplets(node.name, blockId, session);
		if (results != null && results.size() > 0)
		{
			Collections.sort(results, new TripletsTableComparator());
			//			for(int i=0; i<results.size(); i++) {
			//				if(results.get(i).getDatanodeName().equals(node.getName()))
			//					results.get(i).getIndex();
			//			}
			return results.get(0).getIndex(); //FIXME: this should only return a datanode which is connected to this NN
		}
		return -1;
	}

	/*
	 * Find the number of datanodes to which a block of blockId belongs to.
	 * 
	 * This replaces BlockInfo.numNodes().
	 * It iterates through all the triplet rows for a particular
	 * block, finds the highest index such that the datanode
	 * entry is not null, and returns that index+1.
	 */
	public static int numDatanodesForBlock(long blockId)
	{
		Session session = DBConnector.obtainSession();
		List<TripletsTable> results = selectTriplet(session, blockId);
		int count = 0;

		if (results != null && results.size() > 0)
		{
			// Sort by index, so the highest index is last.
			for (TripletsTable t: results)
			{
				if (t.getDatanodeName() != null) //FIXME: [thesis] this should be dnManager.getDatanodeByName(t.getDatanodeName()) != null
				{
					count++;
				}
			}
		}
		return count;
	}

	/*
	 * Find all BlockInfo objects associated with a particular DataNode.
	 * 
	 * This is used by DatanodeDescriptor.BlockIterator
	 */
	public static List<BlockInfo> getBlockListForDatanode (String dataNodeName)
	{
		List<BlockInfo> ret = new ArrayList<BlockInfo>();
		Session session = DBConnector.obtainSession();

		List<TripletsTable> tripletsForDatanode = getTripletsListUsingFieldInternal("datanodeName", dataNodeName, session);
		for (TripletsTable t: tripletsForDatanode)
		{
			ret.add(getBlockInfo(session, t.getBlockId(), false));
		}
		return ret;
	}


	/** Return total blocks in BlockInfo table
	 * @param isTransactional - If its already part of a transaction (true) or not (false)
	 * @return total blocks
	 * @throws ClusterJException 
	 */
	public static int getTotalBlocks()
	{
		Session session = DBConnector.obtainSession();
		int tries = RETRY_COUNT;
		boolean done = false;

		while (done == false && tries > 0)
		{
			try
			{
				int totalBlocks = (int) getTotalBlocksInternal(session);
				done = true;
				return totalBlocks;
			}
			catch (ClusterJException e)
			{
				System.err.println("getTotalBlocks failed " + e.getMessage());
				tries--;
			}
		}
		return 0;
	}

	/**
	 * Reset the value of total blocks - Maybe required at startup or fresh cluster setup
	 */
	public static void resetTotalBlocks(boolean isTransactional)
	{
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		assert tx.isActive() == isTransactional;

		if(isTransactional)
		{
			resetTotalBlocksInternal(session);
		}
		else
		{
			int tries = RETRY_COUNT;
			boolean done = false;

			while (done == false && tries > 0)
			{
				try
				{
					tx.begin();
					resetTotalBlocksInternal(session);
					tx.commit();
					session.flush();
					done = true;
				}
				catch (ClusterJException e)
				{
					tx.rollback();
					System.err.println("Reset of total blocks failed " + e.getMessage());
					tries--;
				}
			}
		}
	}


	/////////////////////////////////////////////////////////////////////////////
	// Internal Functions
	/////////////////////////////////////////////////////////////////////////////

	/** Primary key lookup in the BlockInfo table using block ID
	 * @param session
	 * @param blkid
	 * @return a row from the BlockInfo table
	 */
	private static BlockInfoTable selectBlockInfo(Session session, long blkid) {
		return session.find(BlockInfoTable.class, blkid);
	}

	/** Delete a row from BlockInfo using blockId
	 * @param session
	 * @param blkid
	 */
	private static void deleteBlockInfo(Session session, long blkid) {
		session.deletePersistent(BlockInfoTable.class, blkid);
	}

	/** Insert a row in the BlockInfo table
	 * @param session
	 * @param binfot
	 */
	private static void insertBlockInfo(Session session, BlockInfoTable binfot) {
		session.savePersistent(binfot);
	}

	/**
	 * Updates total blocks in block table
	 * @param session
	 * @param toAdd - If true, we increment total blocks else we decrement
	 */
	private static void updateTotalBlocksInternal(Session session, BlockTotalTable blkTotal)
	{
		session.updatePersistent(blkTotal);
		session.flush();
	}
	/**
	 * Reset the value of total blocks - Maybe required at startup or fresh cluster setup
	 */
	private static void resetTotalBlocksInternal(Session session)
	{
		BlockTotalTable blkTotal = session.newInstance(BlockTotalTable.class);
		blkTotal.setId(BLOCKTOTAL_ID);
		blkTotal.setTotal(0);
		session.makePersistent(blkTotal);
	}


	/** Insert a row in the BlockInfo table
	 * @param session
	 * @param blockID
	 * @param index
	 * @param iNodeID
	 * @param numBytes
	 * @param genStamp
	 * @param replication
	 * @param blockUCState
	 * @param timestamp
	 */
	@SuppressWarnings("unused")
	private static void insertBlockInfo(Session session, boolean update, 
			long blockID, 
			int index, 
			long iNodeID, 
			long numBytes, 
			long genStamp, 
			int replication, 
			int blockUCState,
			long timestamp) {
		BlockInfoTable binfot = session.newInstance(BlockInfoTable.class);
		binfot.setBlockId(blockID);
		binfot.setBlockIndex(index);
		binfot.setINodeID(iNodeID);
		binfot.setNumBytes(numBytes);
		binfot.setGenerationStamp(genStamp);
		binfot.setReplication(replication);
		binfot.setTimestamp(timestamp);
		insertBlockInfo(session, binfot);
	}

	/** Fetch all blocks of an inode from BlockInfo table
	 * @param session
	 * @param iNodeID
	 * @return a List of BlockInfo rows
	 */
	private static List<BlockInfoTable> selectAllBlockInfo(Session session, long iNodeID) {
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
		dobj.where(dobj.get("iNodeID").equal(dobj.param("param")));
		Query<BlockInfoTable> query = session.createQuery(dobj);
		query.setParameter("param", iNodeID);
		return 	query.getResultList();
	}

	/** Fetch a row from the Triplets table using blockId and index
	 * @param session
	 * @param blkid
	 * @param index
	 * @return a row from the Triplets table
	 */
	private static TripletsTable selectTriplet(Session session, long blkid, int index) {
		Object[] pKey = new Object[2];
		pKey[0] = blkid;
		pKey[1] = index;
		return session.find(TripletsTable.class, pKey);
	}

	/** Delete a triplet using blockId and index
	 * @param session
	 * @param blkid
	 * @param index
	 */
	private static void deleteTriplet(Session session, long blkid, int index) {
		Object[] pKey = new Object[2];
		pKey[0] = blkid;
		pKey[1] = index;
		session.deletePersistent(TripletsTable.class, pKey);
	}

	private static void insertTriplet(Session session, TripletsTable tt, boolean update) {

		if(update) {
			session.savePersistent(tt);
			LOG.debug("W: Triplet about to be updated: " + tt.getBlockId() + " DN: " + tt.getDatanodeName());
		}
		else {
			session.makePersistent(tt);
			LOG.debug("W: Triplet about to be inserted: " + tt.getBlockId() + " DN: " + tt.getDatanodeName());
		}
	}

	@SuppressWarnings("unused")
	private static void insertTriplet(Session session, boolean update,
			long blkid,
			int index, 
			String datanodeName) {
		TripletsTable tt = session.newInstance(TripletsTable.class);
		tt.setBlockId(blkid);
		tt.setIndex(index);
		tt.setDatanodeName(datanodeName);
		insertTriplet(session, tt, update);
	}

	/** Fetch a row from the Triplets table using blockId (doesn't do a primary key lookup)
	 * @param session
	 * @param blkid
	 * @return a row from the Triplets table
	 */
	private static List<TripletsTable> selectTriplet(Session session, long blkid) {
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
		dobj.where(dobj.get("blockId").equal(dobj.param("param")));
		Query<TripletsTable> query = session.createQuery(dobj);
		query.setParameter("param", blkid);
		return 	query.getResultList();
	}

	/** Delete a triplet using blockId (doesn't use primary key)
	 * @param session
	 * @param blkid
	 * @param index
	 */
	private static void deleteTriplet(Session session, long blkid) {
		List<TripletsTable> triplets = selectTriplet(session, blkid);
		session.deletePersistentAll(triplets);
	}

	/** Fetch all Triplets for a datanode from the Triplets table
	 * @param session
	 * @param datanodeName
	 * @return a list of triplets
	 */
	private static List<TripletsTable> selectTriplets(Session session, String datanodeName) {
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
		dobj.where(dobj.get("datanodeName").equal(dobj.param("param")));
		Query<TripletsTable> query = session.createQuery(dobj);
		query.setParameter("param", datanodeName);
		return query.getResultList();
	}


	/** Updates a row in BlockInfoTable
	 * @param session
	 * @param bitd
	 */
	private static void updateBlockInfoTableInternal(Session session, BlockInfoTable bit) {
		session.updatePersistent(bit);
                session.flush();
	}

	/** Fetches all rows from the Triplets table using datanodeName and blockId
	 * SELECT * FROM triplets WHERE datanodeName=? AND blockId=?;
	 */
	private static List<TripletsTable> selectTriplets(String datanodeName, long blockId, Session session){

		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
		Predicate pred = dobj.get("datanodeName").equal(dobj.param("param1"));
		Predicate pred2 = dobj.get("blockId").equal(dobj.param("param2"));
		Predicate and = pred.and(pred2);
		dobj.where(and);
		Query<TripletsTable> query = session.createQuery(dobj);
		query.setParameter("param1", datanodeName); //the WHERE clause of SQL
		query.setParameter("param2", blockId);
		return 	query.getResultList();

	}

	/** Returns a list of datanode addresses for a block
	 * @param blockId
	 * @return a list of ip:port pairs if they exist in NDB, and null otherwise
	 */
	public static List<String> getDatanodeAddr(long blockId) {
		int tries = RETRY_COUNT;
		boolean done = false;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				List<TripletsTable> triplets = selectTriplet(session, blockId);
				List<String> ret = new ArrayList<String>();
				for(TripletsTable triplet : triplets) {
					ret.add(triplet.getDatanodeName());
				}
				done=true;
				if(ret.size() > 0)
					return ret;
			}
			catch (ClusterJException e){
				//System.err.println("getDatanodeAddr failed " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		return null;
	}



	/** Return total blocks in BlockInfo table
	 * @param isTransactional - If its already part of a transaction (true) or not (false)
	 * @throws ClusterJException 
	 */
	private static long getTotalBlocksInternal(Session session) throws ClusterJException
	{
		BlockTotalTable blkTable = session.find(BlockTotalTable.class, BLOCKTOTAL_ID);
		return blkTable.getTotal();
	}

}


