package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import se.sics.clusterj.INodeTableSimple;
import se.sics.clusterj.InodeTable;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;

import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

/**
 * This class provides the CRUD operations for inodes stored in database. 
 * It also provides helper methods for conversion from/to HDFS/ClusterJ data structures 
 * All methods ending with "Internal" in this class must be wrapped with tx.begin() and tx.commit(). 
 * This gives us an opportunity to pack multiple operations in a single transaction to reduce round-trips 
 */
public class INodeHelper {
	static final Log LOG = LogFactory.getLog(INodeHelper.class);
	private static FSNamesystem ns = null;
	static final int RETRY_COUNT = 3; 

	/**Sets the FSNamesystem object. This method should be called before using any of the helper functions in this class.
	 * @param fsns an already initialized FSNamesystem object
	 */
	public static void initialize(FSNamesystem fsns) {
		ns = fsns;
	}

	public static void replaceChild (INode thisInode, INode newChild){
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while (done == false && tries > 0) {
			try {
				tx.begin();

				replaceChild(thisInode, newChild, session);
				done = true;

				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("INodeTableSimpleHelper.replaceChild() threw error " + e.getMessage());
				tries--;
			}
		}
	}

	//TODO: for simple, this should update the times of parent also
	private static void replaceChild (INode thisInode, INode newChild, Session session){

		INodeTableSimple inodet = selectINodeTableInternal(session, newChild.getLocalName(), thisInode.getID());
		assert inodet == null : "Child not found in database";

		inodet.setModificationTime(thisInode.modificationTime); 
		inodet.setATime(thisInode.getAccessTime()); 
		inodet.setName(newChild.getLocalName()); 
		DataOutputBuffer permissionString = new DataOutputBuffer();

		try {
			newChild.getPermissionStatus().write(permissionString);
		} catch (IOException e) {
			e.printStackTrace();
		}

		inodet.setPermission(permissionString.getData());
		//inode.setParentID(newChild.getParent().getID()); //FIXME: no need of this roundtrip
		inodet.setParentID(newChild.getParentIDLocal()); 
		inodet.setNSQuota(newChild.getNsQuota());
		inodet.setDSQuota(newChild.getDsQuota());

		if (newChild instanceof INodeDirectory)
		{
			inodet.setIsClosedFile(false);
			inodet.setIsUnderConstruction(false);
			inodet.setIsDirWithQuota(false);    
			inodet.setIsDir(true);
		}
		if (newChild instanceof INodeDirectoryWithQuota)
		{
			inodet.setIsClosedFile(false);
			inodet.setIsDir(false);	    	
			inodet.setIsUnderConstruction(false);
			inodet.setIsDirWithQuota(true);    	
			inodet.setNSCount(((INodeDirectoryWithQuota) newChild).getNsCount());
			inodet.setDSCount(((INodeDirectoryWithQuota) newChild).getDsCount());
		}
		updateINodeTableInternal(session, inodet);
	}


	private static INode convertINodeTableToINode (INodeTableSimple inodetable) throws IOException
	{

		DataInputBuffer buffer = new DataInputBuffer();
		buffer.reset(inodetable.getPermission(), inodetable.getPermission().length);
		PermissionStatus ps = PermissionStatus.read(buffer);

		INode inode = null;

		if (inodetable.getIsDir()){
			String iname = (inodetable.getName().length() == 0) ? INodeDirectory.ROOT_NAME : inodetable.getName();
			inode = new INodeDirectory(iname, ps);
			inode.setAccessTime(inodetable.getATime());
			inode.setModificationTime(inodetable.getModificationTime());
			((INodeDirectory)(inode)).setID(inodetable.getId()); //added for simple
		}
		if (inodetable.getIsDirWithQuota()) {
			inode = new INodeDirectoryWithQuota(inodetable.getName(), ps, inodetable.getNSCount(), inodetable.getDSQuota());
			inode.setAccessTime(inodetable.getATime());
			inode.setModificationTime(inodetable.getModificationTime());
			((INodeDirectoryWithQuota)(inode)).setID(inodetable.getId()); //added for simple
		}
		if (inodetable.getIsUnderConstruction()) {
			//Get the full list of blocks for this inodeID, 
			// at this point no blocks have no INode reference
			BlockInfo [] blocks = new BlockInfo [1];
			blocks[0]= new BlockInfo(1);

			inode = new INodeFileUnderConstruction(inodetable.getName().getBytes(),
					getReplicationFromHeader(inodetable.getHeader()),
					inodetable.getModificationTime(),
					getPreferredBlockSize(inodetable.getHeader()),
					blocks,
					ps,
					inodetable.getClientName(),
					inodetable.getClientMachine(),
					ns.getBlockManager().getDatanodeManager().getDatanodeByHost(inodetable.getClientNode()));

			((INodeFile)(inode)).setID(inodetable.getId()); 

			BlockInfo[] blocksArray = BlocksHelper.getBlocksArrayInternal((INodeFile)inode, DBConnector.obtainSession());
			((INodeFile)(inode)).setBlocksList(blocksArray);
		}
		if (inodetable.getIsClosedFile()) {
			inode = new INodeFile(ps,
					0,
					getReplicationFromHeader(inodetable.getHeader()),
					inodetable.getModificationTime(),
					inodetable.getATime(), getPreferredBlockSize(inodetable.getHeader()));

			//Fixed the header after retrieving the object
			((INodeFile)(inode)).setHeader(inodetable.getHeader());

			((INodeFile)(inode)).setID(inodetable.getId());
			BlockInfo[] blocksArray = BlocksHelper.getBlocksArrayInternal((INodeFile)inode, DBConnector.obtainSession());
			((INodeFile)(inode)).setBlocksList(blocksArray);
		}

		inode.setLocalName(inodetable.getName()); //added for simple
		inode.setParentIDLocal(inodetable.getParentID());

		return inode;
	}

	/** Get the replication value out of the header
	 * 	useful to reconstruct InodeFileUnderConstruction from DB 
	 */
	private static short getReplicationFromHeader(long header) {
		//Number of bits for Block size
		final short blockBits = 48;
		//Format: [16 bits for replication][48 bits for PreferredBlockSize]
		final long headerMask = 0xffffL << blockBits;
		return (short) ((header & headerMask) >> blockBits);
	}
	/**
	 * Return preferredBlockSize for the file
	 * @return
	 */
	private static long getPreferredBlockSize(long header) {
		final short blockBits = 48;
		//Format: [16 bits for replication][48 bits for PreferredBlockSize]
		final long headerMask = 0xffffL << blockBits;
		return header & ~headerMask;
	}




	/////////////////////////////////////////////////////
	//Basic functions, added for Simple
	/////////////////////////////////////////////////////



	/** Fetch a tuple from database using primary key inodeid
	 * @param session
	 * @param inodeid
	 * @return the tuple if it exists, null otherwise
	 */
	private static INodeTableSimple selectINodeTableInternal(Session session, long inodeid) {
		return session.find(INodeTableSimple.class, inodeid);
	}

	/** Fetch a tuple from the database using name|parentID
	 * @param session
	 * @param name
	 * @param parentid
	 * @return the tuple if it exists, null otherwise. 
	 */
	private static INodeTableSimple selectINodeTableInternal(Session session, String name, long parentid) {
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<INodeTableSimple> dobj = qb.createQueryDefinition(INodeTableSimple.class);
		Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
		Predicate pred2 = dobj.get("parentID").equal(dobj.param("parentID"));
		dobj.where(pred1.and(pred2));
		Query<INodeTableSimple> query = session.createQuery(dobj);
		query.setParameter("name", name);
		query.setParameter("parentID", parentid);
		List<INodeTableSimple> results = query.getResultList();
		if(results.size() > 1) {
			LOG.error(results.size() + " row(s) with same name|parentID. Not good!");
			return results.get(0);
		}
		else if(results.size() == 0){
			return null;
		}
		else 
			return results.get(0);
	}

	/** Deletes an inode from the database
	 * @param session
	 * @param inodeid
	 */
	private static void deleteINodeTableInternal(Session session, long inodeid) {
		INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
		session.deletePersistent(inodet);
	}

	/** Deletes an inode from the database
	 * @param session
	 * @param name
	 * @param parentID
	 */
	@SuppressWarnings("unused")
	private static void deleteINodeTableInternal(Session session, String name, long parentID) {
		INodeTableSimple inodet = selectINodeTableInternal(session, name, parentID);
		session.deletePersistent(INodeTableSimple.class, inodet);
	}

	/** Updates an already existing inode in the database
	 * @param session
	 * @param inodet
	 */
	private static void updateINodeTableInternal(Session session, INodeTableSimple inodet) {
		session.updatePersistent(inodet);
	}

	private static void insertINodeTableInternal(Session session, INodeTableSimple inodet) {
		session.makePersistent(inodet);
	}

	@SuppressWarnings("unused")
	private static void insertINodeTableInternal(
			Session session, 
			long id, String name, long parentID, boolean isDir, boolean isDirWithQuota, 
			long modificationTime, long aTime, byte[] permission, long nscount, long dscount,
			long nsquota, long dsquota, boolean isUnderConstruction, String clientName,
			String clientMachine, String clientNode, boolean isCloseFile, long header, byte[] symlink
			) {
		INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, id);
		inodet.setName(name);
		inodet.setParentID(parentID);
		inodet.setIsDir(isDir);
		inodet.setIsDirWithQuota(isDirWithQuota);
		inodet.setModificationTime(modificationTime);
		inodet.setATime(aTime);
		inodet.setPermission(permission);
		inodet.setNSCount(nscount);
		inodet.setDSCount(dscount);
		inodet.setNSQuota(nsquota);
		inodet.setDSQuota(dsquota);
		inodet.setIsUnderConstruction(isUnderConstruction);
		inodet.setClientName(clientName);
		inodet.setClientMachine(clientMachine);
		inodet.setClientNode(clientNode);
		inodet.setIsClosedFile(isCloseFile);
		inodet.setHeader(header);
		inodet.setSymlink(symlink);
		session.makePersistent(inodet);
	}


	////////////////////////
	//New functions for Simple
	////////////////////////

	/**Fetches a fully cooked INode object from the database using name and parentid
	 * @param name
	 * @param parentid
	 * @return an INode object, or null if not found in database
	 */
	public static INode getINode(String name, long parentid) throws IOException{
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				INodeTableSimple inodet = selectINodeTableInternal(session, name, parentid);
				if(inodet != null){
					INode ret = convertINodeTableToINode(inodet);
					done = true;
					return ret;
				}
				return null;
			}
			catch (ClusterJException e){
				System.err.println("INodeTableSimpleHelper.getChildINode() threw error " + e.getMessage());
				tries--;
			} 
		}

		return null;
	}

	/**Fetches a fully cooked INode object from the database using inodeid
	 * @param name
	 * @param parentid
	 * @return an INode object, or null if not found in database
	 */
	public static INode getINode(long inodeid){
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				INodeTableSimple inodet = selectINodeTableInternal(session, inodeid);
				if(inodet != null){
					INode ret = convertINodeTableToINode(inodet);
					done = true;
					return ret;
				}
				return null;
			}
			catch (ClusterJException e){
				System.err.println("INodeHelper.getINode() threw error " + e.getMessage());
				tries--;
			} 
			catch (IOException io) {
				tries--;
				LOG.debug("INodeHelper.getINode()" + io.getMessage());
				io.printStackTrace();
			}
		}

		return null;
	}


	/**Returns all children of a parent
	 * @param parentid
	 * @return a list of children
	 * @throws IOException
	 */
	public static List<INode> getChildren(long parentid) throws IOException {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.obtainSession();
		List<INode> children = new ArrayList<INode>();

		while (done == false && tries > 0) {
			try {
				List<INodeTableSimple> inodetList = getChildrenInternal(session, parentid);
				for(INodeTableSimple inodet : inodetList) {
					children.add(convertINodeTableToINode(inodet));
				}
				if (children.size() > 0 ) {
					done = true;
					return children;
				} 
				return null;
			}
			catch (ClusterJException e){
				System.err.println("INodeTableSimpleHelper.getChildren() threw error " + e.getMessage());
				tries--;
			}
		}

		return null;
	}

	/**Fetches all the children of a parent from the database
	 * @param session
	 * @param parentid
	 * @return a list of children
	 * @throws IOException
	 */
	private static List<INodeTableSimple> getChildrenInternal(Session session, long parentid) throws IOException {
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<INodeTableSimple> dobj = qb.createQueryDefinition(INodeTableSimple.class);
		Predicate pred1 = dobj.get("parentID").equal(dobj.param("parentID"));
		dobj.where(pred1);
		Query<INodeTableSimple> query = session.createQuery(dobj);
		query.setParameter("parentID", parentid);
		List<INodeTableSimple> results = query.getResultList();
		return results;
	}


	/**Updates the modification time of an inode in the database
	 * @param inodeid
	 * @param modTime
	 */
	public static void updateModificationTime(long inodeid, long modTime){
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				updateModificationTimeInternal(session, inodeid, modTime);
				tx.commit();
				done = true;
				session.flush();
			}
			catch (ClusterJException e){
				if(tx.isActive()) 
					tx.rollback();
				System.err.println("updateModificationTime threw error " + e.getMessage());
				tries--;
			}
		}

	}

	/**Internal function for updating the modification time of an inode in the database
	 * @param session
	 * @param inodeid
	 * @param modTime
	 */
	private static void updateModificationTimeInternal(Session session, long inodeid, long modTime) {
		INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
		inodet.setModificationTime(modTime);
		session.updatePersistent(inodet);
	}

	/** Adds a child to a directory
	 * @param node
	 * @param parentid
	 */
	public static void addChild(INode node, boolean isRoot, long parentid){
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while (done == false && tries > 0) {
			try {
				tx.begin();

				addChildInternal(session, node, isRoot, parentid);
				done = true;

				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("INodeTableSimpleHelper.addChild() threw error " + e.getMessage());
				tries--;
			}
		}
	}


	/** Internal function to add a child to a directory after doing all the required casting
	 * @param session
	 * @param node
	 * @param parentid
	 */
	private static void addChildInternal(Session session, INode node, boolean isRoot, long parentid){
		boolean entry_exists;
		//TODO: this check seems redundant, remove this
		INodeTableSimple inode = selectINodeTableInternal(session, node.getLocalName(), parentid);
		entry_exists = true;
		if (inode == null)
		{
			inode = session.newInstance(INodeTableSimple.class);
			inode.setId(isRoot ? 0 : node.getID()); //added for simple
			entry_exists = false;
		}

		inode.setModificationTime(node.modificationTime);
		inode.setATime(node.getAccessTime());
		inode.setName(node.getLocalName()); //modified for simple
		DataOutputBuffer permissionString = new DataOutputBuffer();
		try {
			node.getPermissionStatus().write(permissionString);
		} catch (IOException e) {
			e.printStackTrace();
		}

		inode.setPermission(permissionString.getData());

//		if(!isRoot)
//			inode.setParentID(parentid); //added for simple
		inode.setParentID(parentid);

		inode.setNSQuota(node.getNsQuota());
		inode.setDSQuota(node.getDsQuota());
		if (node instanceof INodeDirectory)
		{
			inode.setIsClosedFile(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);    
			inode.setIsDir(true);
		}
		if (node instanceof INodeDirectoryWithQuota)
		{
			inode.setIsClosedFile(false);
			inode.setIsDir(true); //why was it false earlier?	    	
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(true);    	
			inode.setNSCount(((INodeDirectoryWithQuota) node).getNsCount());
			inode.setDSCount(((INodeDirectoryWithQuota) node).getDsCount());
		}
		if (node instanceof INodeFile)
		{
			inode.setIsDir(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);
			inode.setIsClosedFile(true);
			inode.setHeader(((INodeFile) node).getHeader());
		}
		if (node instanceof INodeFileUnderConstruction)
		{
			inode.setIsClosedFile(false);
			inode.setIsDir(false);
			inode.setIsDirWithQuota(false);
			inode.setIsUnderConstruction(true);
			inode.setClientName(((INodeFileUnderConstruction) node).getClientName());
			inode.setClientMachine(((INodeFileUnderConstruction) node).getClientMachine());
			try {
				inode.setClientNode(((INodeFileUnderConstruction) node).getClientNode().getName());
			} catch (NullPointerException e) { // Can trigger when NN is also the client

			}
		}
		if (node instanceof INodeSymlink)
			inode.setSymlink(((INodeSymlink) node).getSymlink());

		if(isRoot)
			session.savePersistent(inode);
		else if (entry_exists)
			updateINodeTableInternal(session, inode);
		else
			insertINodeTableInternal(session, inode);

	}


	/** Deletes an inode from the database
	 * @param inodeid the inodeid to remove
	 */
	public static /*synchronized*/ void removeChild(long inodeid) {
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while (done == false && tries > 0) {
			try {
				tx.begin();
				deleteINodeTableInternal(session, inodeid);
				done = true;

				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				if(tx.isActive())
					tx.rollback();
				System.err.println("INodeHelper.removeChild() threw error " + e.getMessage());
				tries--;
			}
		}

	}

	/** Updates the header of an inode in database
	 * @param inodeid
	 * @param header
	 * @return
	 * @throws IOException
	 */
	public static boolean updateHeader (long inodeid ,long header) throws IOException{
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		INodeTableSimple inode = selectINodeTableInternal(session, inodeid);
		assert inode == null : "INodeTableSimple object not found";

		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				updateHeaderInternal(session, inodeid, header);
				tx.commit();
				done = true;
				session.flush();
				return done;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("INodeTableSimpleHelper.addChild() threw error " + e.getMessage());
				tries--;
			}
		}

		return false;
	}

	/** Updates the header of an inode in database
	 * @param session
	 * @param inodeid
	 * @param header
	 */
	private static void updateHeaderInternal (Session session, long inodeid, long header){
		INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
		inodet.setHeader(header);
		session.updatePersistent(inodet);
	}

	public static INodeDirectory getParent(long parentid) {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.obtainSession();

		while (done == false && tries > 0) {
			try {
				INodeTableSimple parent_inodet = selectINodeTableInternal(session, parentid);
				INodeDirectory parentDir = (INodeDirectory)convertINodeTableToINode(parent_inodet);
				done = true;
				session.flush();
				return parentDir;
			}
			catch (ClusterJException e){
				System.err.println("INodeTableSimpleHelper.getParent() threw error " + e.getMessage());
				tries--;
			}
			catch (IOException e){
				LOG.error("INodeTableSimpleHelper.getParent() threw error " + e.getMessage());
				tries--;
			}
		}
		return null;
	}

	/** Fetch inodes from the database in one shot (using the ClusterJ "in" clause). 
	 *  This is far better (in terms of latency ) than launching multiple primary key lookups in a transaction
	 *  
	 * @param entries
	 * @return chained inodes, or null if either the chain is broken or some IDs dont exist in database
	 */
	public static INode[] getINodes(INodeEntry[] entries) {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.obtainSession();

		while (done == false && tries > 0) {
			try {
				List<INodeTableSimple>  inodetList = sortINodesByPath(selectINodesInternal(session, entries));
				if(inodetList == null)
					return null;
				else if(inodetList.size() == 0)
					return null;
				else {
					INode[] inodes = new INode[inodetList.size()];
					for(int i=0; i < inodetList.size(); i++) {
						inodes[i] = convertINodeTableToINode(inodetList.get(i));
					}
					return inodes;
				}
			}
			catch (ClusterJException e){
				System.err.println("INodeTableSimpleHelper.getINodes() threw error " + e.getMessage());
				tries--;
			}
			catch (IOException io) {
				tries--;
				LOG.error("INodeHelper.getINode()" + io.getMessage());
				io.printStackTrace();
			}
		}
		return null;
	}
	
	private static List<INodeTableSimple> selectINodesInternal(Session session, INodeEntry[] entries) throws IOException {
		Long[] IDs = new Long[entries.length];
		for (int i = 0; i < entries.length; i++) {
			IDs[i] = entries[i].id; 
		}
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<INodeTableSimple> dobj = qb.createQueryDefinition(INodeTableSimple.class);
		PredicateOperand field = dobj.get("id");
		PredicateOperand values = dobj.param("param");
		Predicate predicate = field.in(values);
		dobj.where(predicate);
		Query<INodeTableSimple> query = session.createQuery(dobj);
		query.setParameter("param", IDs);
		List<INodeTableSimple> results = query.getResultList();
		
		if(results.size() != entries.length) //means the cache is old, so invalidate
			return null;
		return results;

	}

	
	/** Chain the inodes together according to the directory hierarchy
	 *  This is required because the ClusterJ "in" clause doesn't guarantee the order
	 *  
	 * @param inodeList a list of inodets which need to be sorted
	 * @return
	 */
	public static List<INodeTableSimple> sortINodesByPath(List<INodeTableSimple> inodeList) {

		if(inodeList == null)
			return null;
		if(inodeList.size() == 1)
			return inodeList;

		//converting list to a map so that we can get an inode in O(1) during chaining
		Map<Long, INodeTableSimple> inodesMap = new HashMap<Long, INodeTableSimple>(); //<pid, inodetable>
		for(INodeTableSimple inodet : inodeList) {
			inodesMap.put(inodet.getParentID(), inodet);
		}

		List<INodeTableSimple> inodetSorted = 
				new ArrayList<INodeTableSimple>(inodeList.size());
		int count = 0;
		INodeTableSimple root = inodesMap.get(-1L); //use a constant here
		inodetSorted.add(root);
		Long next_parent_id = root.getId();

		//lets chain the inodes together
		while(count < inodeList.size()-1) {
			INodeTableSimple inodet = inodesMap.get(next_parent_id);
			if(inodet == null) //if the chain is broken, return null and invalidate the cache
				return null;
			inodetSorted.add(inodet);
			next_parent_id = inodet.getId();
			count++;
		}
		return inodetSorted;
	}
	

}

//TODO: replace all syserr with LOG.error
