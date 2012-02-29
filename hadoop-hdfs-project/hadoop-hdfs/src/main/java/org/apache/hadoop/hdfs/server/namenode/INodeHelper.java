package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

public class INodeHelper {
	static final Log LOG = LogFactory.getLog(INodeHelper.class);
	static final int MAX_DATA = 128;
	public static FSNamesystem ns = null;
	static final int RETRY_COUNT = 3; 

	public static void replaceChild (INode thisInode, INode newChild){
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while (done == false && tries > 0) {
			try {
				tx.begin();

				replaceChildInternal(thisInode, newChild, session);
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

	//FIXME: for simple, this should update the times of parent also
	public static void replaceChildInternal (INode thisInode, INode newChild, Session session){

		INodeTableSimple inode = selectINodeTableInternal(session, newChild.getLocalName(), thisInode.getID());
		assert inode == null : "Child not found in database";

		inode.setModificationTime(thisInode.modificationTime); 
		inode.setATime(thisInode.getAccessTime()); 
		inode.setName(newChild.getLocalName()); 
		DataOutputBuffer permissionString = new DataOutputBuffer();

		try {
			newChild.getPermissionStatus().write(permissionString);
		} catch (IOException e) {
			e.printStackTrace();
		}

		inode.setPermission(permissionString.getData());
		inode.setParentID(newChild.getParent().getID());
		inode.setNSQuota(newChild.getNsQuota());
		inode.setDSQuota(newChild.getDsQuota());

		if (newChild instanceof INodeDirectory)
		{
			inode.setIsClosedFile(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);    
			inode.setIsDir(true);
		}
		if (newChild instanceof INodeDirectoryWithQuota)
		{
			inode.setIsClosedFile(false);
			inode.setIsDir(false);	    	
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(true);    	
			inode.setNSCount(((INodeDirectoryWithQuota) newChild).getNsCount());
			inode.setDSCount(((INodeDirectoryWithQuota) newChild).getDsCount());
		}

		session.updatePersistent(inode);
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
			LOG.debug("INodeFileUnderConstruction");
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
			LOG.debug("Closed File");
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

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//Basic functions, added for Simple
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
			LOG.error(results.size() + " row(s) with same name|parentID. FATAL.");
			return results.get(0);
		}
		else if(results.size() == 0){
			LOG.info("Not found in database: " + name + "|" + parentid);
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
		if(LOG.isDebugEnabled())
			LOG.debug("deleteINodeTableInternal:"+inodeid);
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
				System.err.println("INodeTableSimpleHelper.getINode() threw error " + e.getMessage());
				tries--;
			} 
			catch (IOException io) {
				tries--;
				LOG.debug("INodeTableSimpleHelper.getINode()" + io.getMessage());
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

		if(!isRoot)
			inode.setParentID(parentid); //added for simple

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
	
	
	public static synchronized void removeChild(long inodeid) {
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

	private static void updateHeaderInternal (Session session, long inodeid, long header){
		INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
		inodet.setHeader(header);
		session.updatePersistent(inodet);
	}
	
	public static INodeDirectory getParent(long inodeid) {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.obtainSession();

		while (done == false && tries > 0) {
			try {
				INodeTableSimple inodet = selectINodeTableInternal(session, inodeid);
				INodeTableSimple parent_inodet = selectINodeTableInternal(session, inodet.getParentID());
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

}

//TODO: replace all syserr with LOG.error
