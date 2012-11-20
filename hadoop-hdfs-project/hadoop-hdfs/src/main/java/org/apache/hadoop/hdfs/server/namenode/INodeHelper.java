package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.persistance.DBConnector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import se.sics.clusterj.INodeTableSimple;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;

import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.metrics.HelperMetrics;
import org.apache.hadoop.hdfs.server.namenode.INodeCachedEntry;
import se.sics.clusterj.BlockInfoTable;

/**
 * This class provides the CRUD operations for inodes stored in database. 
 * It also provides helper methods for conversion from/to HDFS/ClusterJ data structures 
 * All methods ending with "Internal" in this class must be wrapped with DBConnector.beginTransaction() and DBConnector.commit(). 
 * This gives us an opportunity to pack multiple operations in a single transaction to reduce round-trips 
 */
public class INodeHelper {

	static final Log LOG = LogFactory.getLog(INodeHelper.class);
	private static DatanodeManager dnm = null;
	static final int RETRY_COUNT = 3;

	/**Sets the FSNamesystem object. This method should be called before using any of the helper functions in this class.
	 * @param fsns an already initialized FSNamesystem object
	 */
	public static void initialize(DatanodeManager dnm) {
		INodeHelper.dnm = dnm;
	}

	public static void replaceChild(INode thisInode, INode newChild, boolean isTransactional) {
            DBConnector.checkTransactionState(isTransactional);
            
            if (isTransactional) {
                Session session = DBConnector.obtainSession();
                replaceChildInternal(thisInode, newChild, session);
                session.flush();
            } else {
                replaceChildWithTransaction(thisInode, newChild);
            }
        }

	public static void replaceChildWithTransaction(INode thisInode, INode newChild) {
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while (done == false && tries > 0) {
			try {
				DBConnector.beginTransaction();

                replaceChildInternal(thisInode, newChild, session);

                DBConnector.commit();
                session.flush();
                done = true;
            } catch (ClusterJException e) {
                LOG.error(e.getMessage(), e);
                DBConnector.safeRollback();
                tries--;
            }
        }
    }
	private static void replaceChildInternal(INode thisInode, INode newChild, Session session) {

        INodeTableSimple inodet = selectINodeTableInternal(session, newChild.getLocalName(), thisInode.getID());
        assert inodet != null : "Child not found in database";

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

		if (newChild instanceof INodeDirectory) {
			inodet.setIsClosedFile(false);
			inodet.setIsUnderConstruction(false);
			inodet.setIsDirWithQuota(false);
			inodet.setIsDir(true);
			inodet.setNSCount(((INodeDirectory) newChild).getNsCount());
                        inodet.setDSCount(((INodeDirectory) newChild).getDsCount());
		}
		if (newChild instanceof INodeDirectoryWithQuota) {
			inodet.setIsClosedFile(false);
			inodet.setIsDir(true);
			inodet.setIsUnderConstruction(false);
			inodet.setIsDirWithQuota(true);
		}
		updateINodeTableInternal(session, inodet);
	}

  private static INode convertINodeTableToINode(INodeTableSimple inodetable) throws IOException {

    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(inodetable.getPermission(), inodetable.getPermission().length);
    PermissionStatus ps = PermissionStatus.read(buffer);

    INode inode = null;

    if (inodetable.getIsDir()) {
      if (inodetable.getIsDirWithQuota()) {
        inode = new INodeDirectoryWithQuota(inodetable.getName(), ps, inodetable.getNSQuota(), inodetable.getDSQuota());
      }
      else {
        String iname = (inodetable.getName().length() == 0) ? INodeDirectory.ROOT_NAME : inodetable.getName();
        inode = new INodeDirectory(iname, ps);
      }

      inode.setAccessTime(inodetable.getATime());
      inode.setModificationTime(inodetable.getModificationTime());
      ((INodeDirectory) (inode)).setID(inodetable.getId()); //added for simple
      ((INodeDirectory) (inode)).setSpaceConsumed(inodetable.getNSCount(), inodetable.getDSCount());
      ((INodeDirectory) (inode)).setID(inodetable.getId()); //added for simple
    }
    if (inodetable.getIsUnderConstruction()) {

      inode = new INodeFileUnderConstruction(inodetable.getName().getBytes(),
                                             getReplicationFromHeader(inodetable.getHeader()),
                                             inodetable.getModificationTime(),
                                             getPreferredBlockSize(inodetable.getHeader()),
                                             ps,
                                             inodetable.getClientName(),
                                             inodetable.getClientMachine(),
                                             dnm.getDatanodeByHost(inodetable.getClientNode()));

      ((INodeFile) (inode)).setID(inodetable.getId());
      ((INodeFile) (inode)).setLastBlockId(inodetable.getLastBlockId());

      //List<BlockInfo> blocks = EntityManager.getInstance().findBlocksByInodeId(inode.getID());
      //((INodeFile) (inode)).setBlocks(blocks);
    }
    if (inodetable.getIsClosedFile()) {
      inode = new INodeFile(ps,
                            getReplicationFromHeader(inodetable.getHeader()),
                            inodetable.getModificationTime(),
                            inodetable.getATime(), getPreferredBlockSize(inodetable.getHeader()));

      //Fixed the header after retrieving the object
      ((INodeFile) (inode)).setHeader(inodetable.getHeader());

      ((INodeFile) (inode)).setID(inodetable.getId());
      ((INodeFile) (inode)).setLastBlockId(inodetable.getLastBlockId());
    }

    if (inodetable.getSymlink() != null) {
//      String linkValue = DFSUtil.bytes2String(inodetable.getSymlink());
      inode = new INodeSymlink(inodetable.getSymlink(), inodetable.getModificationTime(),
                               inodetable.getATime(), ps);
      inode.setID(inodetable.getId());
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
            
            HelperMetrics.inodeMetrics.incrSelectUsingPKey();
//              DBConnector.checkTransactionState(true);
//              INodeTableSimple inode = session.newInstance(INodeTableSimple.class, inodeid);
//              session.load(inode);
//              try {
//                // get the inode
//                session.flush();
//              }
//              catch(ClusterJException ex) {
//                // not found
//                return null;
//              }
//              return inode;
            INodeTableSimple inode = session.find(INodeTableSimple.class, inodeid);
            //System.out.println(TransactionContext.printThreadStackTrace());
            return inode;
	}
        
        public static BlockInfoTable doesBlockExist(Session session, long blockId) {
          return session.find(BlockInfoTable.class, blockId);
        }

  /** Fetch a tuple from the database using name|parentID
   * @param session
   * @param name
   * @param parentid
   * @return the tuple if it exists, null otherwise. 
   */
  private static INodeTableSimple selectINodeTableInternal(Session session, String name, long parentid) {
    HelperMetrics.inodeMetrics.incrSelectUsingIndex();

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<INodeTableSimple> dobj = qb.createQueryDefinition(INodeTableSimple.class);
    Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
    Predicate pred2 = dobj.get("parentID").equal(dobj.param("parentID"));
    dobj.where(pred1.and(pred2));

    Query<INodeTableSimple> query = session.createQuery(dobj);
    query.setParameter("name", name);
    query.setParameter("parentID", parentid);
    List<INodeTableSimple> results = query.getResultList();
    if (results.size() > 1) {
      LOG.error(results.size() + " row(s) with same name|parentID. Not good!");
      return results.get(0);
    }
    else if (results.size() == 0) {
      return null;
    }
    else {
      return results.get(0);
    }
  }
  
  /** Deletes an inode from the database
   * @param session
   * @param inodeid
   */
  private static void deleteINodeTableInternal(Session session, long inodeid) {

    HelperMetrics.inodeMetrics.incrDelete();

    LOG.debug("Removing " + inodeid);
    INodeTableSimple inodet = session.find(INodeTableSimple.class, inodeid);
    if(inodet != null) {
      session.deletePersistent(inodet);
    }
  }

  /** Updates an already existing inode in the database
   * @param session
   * @param inodet
   */
  private static void updateINodeTableInternal(Session session, INodeTableSimple inodet) {
    
    HelperMetrics.inodeMetrics.incrUpdate();
    session.updatePersistent(inodet);
  }

	private static void insertINodeTableInternal(Session session, INodeTableSimple inodet) {
    
    HelperMetrics.inodeMetrics.incrInsert();
    
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
	public static INode getINode(String name, long parentid) throws IOException {
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		while (done == false && tries > 0) {
			try {
				INodeTableSimple inodet = selectINodeTableInternal(session, name, parentid);
				if (inodet != null) {
					INode ret = convertINodeTableToINode(inodet);
					done = true;
					return ret;
				}
				return null;
			} catch (ClusterJException e) {
				LOG.error(e.getMessage(), e);
				tries--;
			}
		}

		return null;
	}

/** Fetch a tuple from the database using name|parentID
   * @param session
   * @param name
   * @param parentid
   * @return the tuple if it exists, null otherwise. 
   */
  public static INode getInode(String absolutePathToFile) {
    
    Session session = DBConnector.obtainSession();
    
    String [] filenames = absolutePathToFile.split("/");
    long cur_parentId = 0;
    
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<INodeTableSimple> dobj = qb.createQueryDefinition(INodeTableSimple.class);
    Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
    Predicate pred2 = dobj.get("parentID").equal(dobj.param("parentID"));
    dobj.where(pred1.and(pred2));
    Query<INodeTableSimple> query = session.createQuery(dobj);
    
    INodeTableSimple dbinode = null;
    int index = 1;
    while(index < filenames.length) {
      query.setParameter("name", filenames[index++]);
      query.setParameter("parentID", cur_parentId);
      List<INodeTableSimple> results = query.getResultList();
      
      if(results.isEmpty()) {
        // invalid directory
        return null;
      }
       dbinode = results.get(0);
      //session.flush();
      cur_parentId =dbinode.getId();
    }
    
    INode inode = null;
    try {
      inode = convertINodeTableToINode(dbinode);
    }
    catch(IOException ex) {
      LOG.error("Error on converting to Inode", ex);
    }
    return inode;
  }
                      
                      
  /**Checks whether an inode exists for a given inodeId
   * @param inodeid - id of the inode file
   * @return an INode object, or null if not found in database
   */
  public static boolean exists(long inodeid) {
    INodeTableSimple inodet = selectINodeTableInternal(DBConnector.obtainSession(), inodeid);
    if (inodet != null) {
      return true;
    }
    else {
      return false;
    }
  }

  /**Fetches a fully cooked INode object from the database using inodeid
   * @param name
   * @param parentid
   * @return an INode object, or null if not found in database
   */
  public static INode getINode(long inodeid) {
    INodeTableSimple inodet = selectINodeTableInternal(DBConnector.obtainSession(), inodeid);
    try {
      if (inodet != null) {
        INode ret = convertINodeTableToINode(inodet);
        if (ret != null) {
          // got the Inode
          return ret;
        }
      }
    }
    catch(IOException ex) {
      LOG.error("IOException while fetching INode ["+inodeid+"] from db", ex);
    }
    return null;
  }
//  public static INode getINode(long inodeid) {
//    boolean done = false;
//    int tries = RETRY_COUNT;
//
//    Session session = DBConnector.obtainSession();
//    while (done == false && tries > 0) 
//    {
//      try {
//        INodeTableSimple inodet = selectINodeTableInternal(session, inodeid);
//        if (inodet != null) {
//          INode ret = convertINodeTableToINode(inodet);
//          if(ret == null) {
//            //done = false;
//          }
//          done = true;
//          return ret;
//        }
//        return null;
//      }
//      catch (ClusterJException e) {
//        LOG.error(e.getMessage(), e);
//        tries--;
//      }
//      catch (IOException io) {
//        tries--;
//        LOG.debug("INodeHelper.getINode()" + io.getMessage());
//        io.printStackTrace();
//      }
//    }// end-while
//
//    return null;
//  }

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
				for (INodeTableSimple inodet : inodetList) {
					children.add(convertINodeTableToINode(inodet));
				}
				if (children.size() > 0) {
					done = true;
					sortChildren(children);
					return children;
				}
				return null;
			} catch (ClusterJException e) {
				LOG.error(e.getMessage(), e);
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

    HelperMetrics.inodeMetrics.incrSelectUsingIndex();

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
  public static void updateModificationTime(long inodeid, long modTime, boolean isTransactional) throws ClusterJException {
    DBConnector.checkTransactionState(isTransactional);

    if (isTransactional) {
      Session session = DBConnector.obtainSession();
      updateModificationTimeInternal(session, inodeid, modTime);
      //session.flush();
    } else {
      updateModificationTimeWithTransaction(inodeid, modTime);
    }
  }

  /**Updates the (last) block id for an inode
   * @param inodeid
   * @param blockid
   */
  public static void updateLastBlockIdOnINode(long inodeid, long blockId) throws ClusterJException {
    DBConnector.checkTransactionState(true);

    Session session = DBConnector.obtainSession();
    updateLastBlockIdOnINodeInternal(session, inodeid, blockId);
  }
	/**Updates the modification time of an inode in the database
	 * @param inodeid
	 * @param modTime
	 */
	private static void updateModificationTimeWithTransaction(long inodeid, long modTime) {
		boolean done = false;
		int tries = RETRY_COUNT;

		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				DBConnector.beginTransaction();
				updateModificationTimeInternal(session, inodeid, modTime);
				DBConnector.commit();
				done = true;
				//session.flush();
			} catch (ClusterJException e) {
				if (tx.isActive()) {
					DBConnector.safeRollback();
				}
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
	private static void updateModificationTimeInternal(Session session, long inodeid, long modTime) throws ClusterJException{
            
            HelperMetrics.inodeMetrics.incrUpdate();
            
		INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
		inodet.setModificationTime(modTime);
		session.updatePersistent(inodet);
	}
	/**Internal function for updating the last block id of an inode in the database
	 * @param session
	 * @param inodeid
	 * @param blockid
	 */
	private static void updateLastBlockIdOnINodeInternal(Session session, long inodeid, long blockId) throws ClusterJException{
            
            HelperMetrics.inodeMetrics.incrUpdate();
            
		INodeTableSimple inodet = selectINodeTableInternal(session, inodeid);
		inodet.setLastBlockId(blockId);
		session.updatePersistent(inodet);
	}

    /** Adds a child to a directory
     * @param node
     * @param parentid
     */
    public static void addChild(INode node, boolean isRoot, long parentid, boolean isTransactional) {
        DBConnector.checkTransactionState(isTransactional);

        if (isTransactional) {
            Session session = DBConnector.obtainSession();
            addChildInternal(session, node, isRoot, parentid);
            //session.flush();
        } else {
            addChildWithTransaction(node, isRoot, parentid);
        }
    }

    /** Adds a child to a directory
     * @param node
     * @param parentid
     */
    private static void addChildWithTransaction(INode node, boolean isRoot, long parentid) {
        boolean done = false;
        int tries = RETRY_COUNT;

        Session session = DBConnector.obtainSession();
        Transaction tx = session.currentTransaction();

        while (done == false && tries > 0) {
            try {
                DBConnector.beginTransaction();

                addChildInternal(session, node, isRoot, parentid);
                DBConnector.commit();
                session.flush();
                done = true;
            } catch (ClusterJException e) {
                LOG.error(e.getMessage(), e);
                if (tx.isActive())
                    DBConnector.safeRollback();
                tries--;
            }
        }
    }

	
	/** Internal function to add a child to a directory after doing all the required casting
	 * @param session
	 * @param node
	 * @param parentid
	 */
	private static void addChildInternal(Session session, INode node, boolean isRoot, long parentid) {
		boolean entry_exists = false;
		//TODO: this check seems redundant, remove this
                                                /*
		INodeTableSimple inode = selectINodeTableInternal(session, node.getID());
		entry_exists = true;
		if (inode == null) {
			inode = session.newInstance(INodeTableSimple.class);
			inode.setId(isRoot ? 0 : node.getID()); //added for simple
			entry_exists = false;
		}
                                                */
                                                INodeTableSimple inode = session.newInstance(INodeTableSimple.class);
                                                inode.setId(isRoot ? 0 : node.getID()); //added for simple
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
		if (node instanceof INodeDirectory) {
			inode.setIsClosedFile(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);
			inode.setIsDir(true);
			inode.setNSCount(((INodeDirectory) node).getNsCount());
                        inode.setDSCount(((INodeDirectory) node).getDsCount());
		}
		if (node instanceof INodeDirectoryWithQuota) {
			inode.setIsClosedFile(false);
			inode.setIsDir(true); //why was it false earlier?	    	
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(true);
		}
		if (node instanceof INodeFile) {
			inode.setIsDir(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);
			inode.setIsClosedFile(true);
			inode.setHeader(((INodeFile) node).getHeader());
		}
		if (node instanceof INodeFileUnderConstruction) {
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
		if (node instanceof INodeSymlink) {
      String linkValue = DFSUtil.bytes2String(((INodeSymlink) node).getSymlink());
			inode.setSymlink(linkValue);
		}

		if (isRoot) {
			session.savePersistent(inode);
		} else if (entry_exists) {
			updateINodeTableInternal(session, inode);
		} else {
			insertINodeTableInternal(session, inode);
		}

	}
  /** Deletes an inode from the database 
   * @param inodeid the inodeid to remove
   */
  public static /*synchronized*/ void removeChild(long inodeid, boolean isTransactional) {
    DBConnector.checkTransactionState(isTransactional);

    if (isTransactional) {
      Session session = DBConnector.obtainSession();
      deleteINodeTableInternal(session, inodeid);
      session.flush();
    } else {
      removeChildWithTransaction(inodeid);
    }
  }

/** Deletes an inode from the database
     * @param inodeid the inodeid to remove
     */
    private static void removeChildWithTransaction(long inodeid) {
        boolean done = false;
        int tries = RETRY_COUNT;

        Session session = DBConnector.obtainSession();
        Transaction tx = session.currentTransaction();

        while (done == false && tries > 0) {
            try {
                DBConnector.beginTransaction();
                deleteINodeTableInternal(session, inodeid);
                DBConnector.commit();
                done = true;
                session.flush();
            } catch (ClusterJException e) {
                if (tx.isActive()) {
                    DBConnector.safeRollback();
                }
                LOG.error(e.getMessage(), e);
                tries--;
            }
        }
    }


	 /**
   * Updates the header of an inode in database
   * @param inodeid
   * @param header
   * @param isTransactional This operation is a part of a transaction.
   */
  public static void updateHeader(long inodeid, long header, boolean isTransactional) {
    Session session = DBConnector.obtainSession();
    boolean isActive = session.currentTransaction().isActive();
    assert isActive == isTransactional :
            "Current transaction's isActive value is " + isActive
            + " but isTransactional's value is " + isTransactional;

    if (isTransactional) {
      updateHeaderInternal(session, inodeid, header);
      session.flush();
    } else {
      updateHeaderOld(inodeid, header);
    }
  }


	/** Updates the header of an inode in database
	 * @param session
	 * @param inodeid
	 * @param header
	 */
	private static void updateHeaderInternal(Session session, long inodeid, long header) {
		INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
		inodet.setHeader(header);
		updateINodeTableInternal(session, inodet);
	}
	
    /** Updates the header of an inode in database
     * @param inodeid
     * @param header
     * @return
     * @throws IOException
     */
    public static boolean updateHeaderOld(long inodeid, long header) {
        boolean done = false;
        int tries = RETRY_COUNT;

        Session session = DBConnector.obtainSession();
        INodeTableSimple inode = selectINodeTableInternal(session, inodeid);
//        assert inode == null : "INodeTableSimple object not found";

        Transaction tx = session.currentTransaction();
        while (done == false && tries > 0) {
            try {
                DBConnector.beginTransaction();
                updateHeaderInternal(session, inodeid, header);
                DBConnector.commit();
                done = true;
                session.flush();
                return done;
            } catch (ClusterJException e) {
                DBConnector.safeRollback();
                System.err.println("INodeTableSimpleHelper.addChild() threw error " + e.getMessage());
                tries--;
            }
        }

        return false;
    }


	public static INodeDirectory getParent(long parentid) {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.obtainSession();

		while (done == false && tries > 0) {
			try {
				INodeTableSimple parent_inodet = selectINodeTableInternal(session, parentid);
				INodeDirectory parentDir = (INodeDirectory) convertINodeTableToINode(parent_inodet);
				done = true;
				session.flush();
				return parentDir;
			} catch (ClusterJException e) {
				System.err.println("INodeTableSimpleHelper.getParent() threw error " + e.getMessage());
				tries--;
			} catch (IOException e) {
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
				List<INodeTableSimple> inodetList = sortINodesByPath(selectINodesInternal(session, entries));
				if (inodetList == null) {
					return null;
				} else if (inodetList.size() == 0) {
					return null;
				} else {
					INode[] inodes = new INode[inodetList.size()];
					for (int i = 0; i < inodetList.size(); i++) {
						inodes[i] = convertINodeTableToINode(inodetList.get(i));
					}
					return inodes;
				}
			} catch (ClusterJException e) {
				System.err.println("INodeTableSimpleHelper.getINodes() threw error " + e.getMessage());
				tries--;
			} catch (IOException io) {
				tries--;
				LOG.error("INodeHelper.getINode()" + io.getMessage());
				io.printStackTrace();
			}
		}
		return null;
	}

        
        /**
         * used for validating the cached inode entries against the database and verifying it with the path
         */
  public static boolean verify(ListOrderedMap<Long, INodeCachedEntry> cachedEntries, INode[] inodesDB) {
    boolean done = false;
    int tries = RETRY_COUNT;
    Session session = DBConnector.obtainSession();

    while (done == false && tries > 0) {
      try {
        //List<INodeTableSimple> dbEntries = selectINodesInternalByLookup(session, cachedEntries.asList().toArray(new Long[cachedEntries.size()]));
        List<INodeTableSimple> dbEntries = selectINodesInternal(session, cachedEntries.asList().toArray(new Long[cachedEntries.size()]));
        if (dbEntries == null) {
          // this means that some or all of the inodes don't exist in the database
          // cache is invalid
          return false;
        }
        else if (dbEntries.isEmpty()) {
          // this means that some or all of the inodes don't exist in the database
          // cache is invalid
          return false;
        }
        else {
          // verification logic: 
          // Loop the dbEntries and match the name and parentid of the cached entries to see if they are still valid
          for(INodeTableSimple dbrecord : dbEntries) {
            INodeCachedEntry cachedrecord = cachedEntries.get(dbrecord.getId());
            if((dbrecord.getName().compareTo(cachedrecord.getName()) != 0) ||    // match name
                  (dbrecord.getParentID() != cachedrecord.getParentid())) { // match parent id
              return false;
            }// end-if
            
            // We need actual INode from db for the file in 'path'
            // the index of key in 'cachedEntries' should correspond to the indexes in the 'inodesDB' array
            inodesDB[cachedEntries.indexOf(dbrecord.getId())] = convertINodeTableToINode(dbrecord);
            //if(cachedEntries.lastKey() == cachedrecord.getId()) {
              //inodesDB[0] = convertINodeTableToINode(dbrecord);
            //}
          }//end-for
          
          return true;
        }// end-main if
      } // end-main try/catch
      catch (ClusterJException e) {
        System.err.println("INodeTableSimpleHelper.getINodes() threw error " + e.getMessage());
        tries--;
      }
      catch (IOException io) {
        tries--;
        LOG.error("INodeHelper.getINode()" + io.getMessage());
        io.printStackTrace();
      }
    }// end-while
    return false;
  }

  private static List<INodeTableSimple> selectINodesInternal(Session session, INodeEntry[] entries) throws IOException {

    HelperMetrics.inodeMetrics.incrSelectUsingIn();

    Long[] IDs = new Long[entries.length];
    for (int i = 0; i < entries.length; i++) {
      IDs[i] = entries[i].id;
    }
    return selectINodesInternal(session, IDs);
  }

  private static List<INodeTableSimple> selectINodesInternalByLookup(Session session, Long[] IDs) throws IOException {

    List<INodeTableSimple> results = new ArrayList<INodeTableSimple>(IDs.length);
    for(Long inodeId : IDs) {
      results.add(selectINodeTableInternal(session, inodeId));
    }

    if (results.size() != IDs.length) {
      return null;
    }
    return results;

  }
  private static List<INodeTableSimple> selectINodesInternal(Session session, Long[] IDs) throws IOException {

    HelperMetrics.inodeMetrics.incrSelectUsingIn();

    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<INodeTableSimple> dobj = qb.createQueryDefinition(INodeTableSimple.class);
    PredicateOperand field = dobj.get("id");
    PredicateOperand values = dobj.param("param");
    Predicate predicate = field.in(values);
    dobj.where(predicate);
    Query<INodeTableSimple> query = session.createQuery(dobj);
    query.setParameter("param", IDs);
    List<INodeTableSimple> results = query.getResultList();

    if (results.size() != IDs.length) {
      return null;
    }
    return results;

  }

	/** Chain the inodes together according to the directory hierarchy
	 *  This is required because the ClusterJ "in" clause doesn't guarantee the order
	 *  
	 * @param inodeList a list of inodets which need to be sorted
	 * @return
	 */
	public static List<INodeTableSimple> sortINodesByPath(List<INodeTableSimple> inodeList) {

		if (inodeList == null) {
			return null;
		}
		if (inodeList.size() == 1) {
			return inodeList;
		}

		//converting list to a map so that we can get an inode in O(1) during chaining
		Map<Long, INodeTableSimple> inodesMap = new HashMap<Long, INodeTableSimple>(); //<pid, inodetable>
		for (INodeTableSimple inodet : inodeList) {
			inodesMap.put(inodet.getParentID(), inodet);
		}

		List<INodeTableSimple> inodetSorted =
				new ArrayList<INodeTableSimple>(inodeList.size());
		int count = 0;
		INodeTableSimple root = inodesMap.get(-1L); //use a constant here
		inodetSorted.add(root);
		Long next_parent_id = root.getId();

		//lets chain the inodes together
		while (count < inodeList.size() - 1) {
			INodeTableSimple inodet = inodesMap.get(next_parent_id);
			if (inodet == null) //if the chain is broken, return null and invalidate the cache
			{
				return null;
			}
			inodetSorted.add(inodet);
			next_parent_id = inodet.getId();
			count++;
		}
		return inodetSorted;
	}
        
 
    public static void updateQuota(INodeDirectoryWithQuota inode, long nsQuota, long dsQuota, boolean isTransactional)
    {
        DBConnector.checkTransactionState(isTransactional);
        inode.setQuota(nsQuota, dsQuota);
        if (isTransactional)
        {
            Session session = DBConnector.obtainSession();
            updateQuotaInternal(session,
                    inode.getID(), inode.getNsQuota(), inode.getDsQuota());
            session.flush();
        }
        else
            updateQuotaWithTransaction(inode.getID(), 
                    inode.getNsQuota(), inode.getDsQuota());
    }
    

    private static void updateQuotaWithTransaction(long inodeId, long nsQuota, long dsQuota) {
        boolean done = false;
        int tries = RETRY_COUNT;

        Session session = DBConnector.obtainSession();
        Transaction tx = session.currentTransaction();

        while (done == false && tries > 0) {
            try {
                DBConnector.beginTransaction();
                updateQuotaInternal(session, inodeId, nsQuota, dsQuota);
                DBConnector.commit();
                session.flush();
                done = true;
            } catch (ClusterJException e) {
                DBConnector.safeRollback();
                System.err.println("INodeTableSimpleHelper.addChild() threw error " + e.getMessage());
                tries--;
            }
        }
    }

    private static void updateQuotaInternal(Session session, long inodeId, long nsQuota, long dsQuota) {
        INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeId);
        inodet.setNSQuota(nsQuota);
        inodet.setDSQuota(dsQuota);
        session.updatePersistent(inodet);
    }


	
    /**
     * Updates number of items and diskspace of the inode directory in DB.
     * @param inodeId
     * @param nsDelta
     * @param dsDelta 
     */
    public static void updateNumItemsInTree(INodeDirectory inode, long nsDelta, long dsDelta, boolean isTransactional)
    {
        DBConnector.checkTransactionState(isTransactional);
        inode.updateNumItemsInTree(nsDelta, dsDelta);
        if (isTransactional)
        {
            Session session = DBConnector.obtainSession();
            updateNumItemsInTreeInternal(session,
                    inode.getID(), inode.getNsCount(), inode.getDsCount());
            //session.flush();
        }
        else
            updateNumItemsInTreeWithTransaction(inode.getID(), 
                    inode.getNsCount(), inode.getDsCount());
    }
    
    /** Adds a child to a directory
     * @param node
     * @param parentid
     */
    private static void updateNumItemsInTreeWithTransaction(long inodeId, long nsDelta, long dsDelta) {
        boolean done = false;
        int tries = RETRY_COUNT;

        Session session = DBConnector.obtainSession();
        Transaction tx = session.currentTransaction();

        while (done == false && tries > 0) {
            try {
                DBConnector.beginTransaction();
                updateNumItemsInTreeInternal(session, inodeId, nsDelta, dsDelta);
                DBConnector.commit();
                session.flush();
                done = true;
            } catch (ClusterJException e) {
                DBConnector.safeRollback();
                System.err.println("INodeTableSimpleHelper.addChild() threw error " + e.getMessage());
                tries--;
            }
        }
    }


    /**
     * Updates number of items and diskspace of the inode directory in DB.
     * @param session
     * @param inodeId
     * @param nsDelta
     * @param dsDelta 
     */
    private static void updateNumItemsInTreeInternal(Session session, long inodeId, long nsDelta, long dsDelta) {
        INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeId);
        inodet.setNSCount(nsDelta);
        inodet.setDSCount(dsDelta);
        updateINodeTableInternal(session, inodet);
    }


     /**
     * Uses DB to set the permission.
     * @param inode
     * @param permissions
     * @throws ClusterJException 
     */
    public static void setPermission(long inodeId, PermissionStatus permissionStatus, boolean isTransactional) throws IOException {
        Session session = DBConnector.obtainSession();
        boolean isActive = session.currentTransaction().isActive();
        assert isActive == isTransactional : 
                "Current transaction's isActive value is " + isActive + 
                " but isTransactional's value is " + isTransactional;
        
        if (isTransactional)
        {
            setPermissionInternal(session, inodeId, permissionStatus);
            session.flush();
        }
        else
            setPermissionWithTransaction(inodeId, permissionStatus);
    }
    
    /**
     * Update the permission in DB using a transaction.
     * @param inodeId
     * @param permissionStatus
     * @return
     * @throws IOException 
     */
    private static boolean setPermissionWithTransaction(long inodeId, PermissionStatus permissionStatus) throws IOException{
        boolean done = false;
        int tries = RETRY_COUNT;

        Session session = DBConnector.obtainSession();

        Transaction tx = session.currentTransaction();
        while (done == false && tries > 0) {
            try {
                DBConnector.beginTransaction();
                setPermissionInternal(session, inodeId, permissionStatus);
                DBConnector.commit();
                session.flush();
                done = true;
                return done;
            } catch (ClusterJException e) {
                DBConnector.safeRollback();
                LOG.error(e.getMessage(), e);
                tries--;
            }
        }

        return false;
    }
    
    /**
     * Internal operation to set permission.
     * @param session
     * @param inodeId
     * @param permissionStatus
     * @throws IOException 
     */
    private static void setPermissionInternal(Session session, long inodeId, PermissionStatus permissionStatus) throws IOException {

        INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeId);
        DataOutputBuffer permissionString = new DataOutputBuffer();
        permissionStatus.write(permissionString);
        inodet.setPermission(permissionString.getData());
        updateINodeTableInternal(session, inodet);
    }
    
    /** Sorts the sibling inodes according to their natural order
     *  This sorting is required for namesystem.getListing()
     *  This is a destructive method
     * @param childs List of siblings
     */
    private static void sortChildren(List<INode> childs) {
	  Collections.sort(childs,
	      new Comparator<INode>() {
	    @Override
	    public int compare(INode o1, INode o2) {
	      return o1.compareTo(o2.name);
	    }
	  }
	      );
	}
    
    
    /**Updates the access time of an inode in the database
     * @param inodeid
     * @param modTime
     */
    public static void updateAccessTime(long inodeid, long aTime, boolean isTransactional) throws ClusterJException
    {
        DBConnector.checkTransactionState(isTransactional);
    
        if (isTransactional)
        {
            Session session = DBConnector.obtainSession();
            updateAccessTimeInternal(session, inodeid, aTime);
            //session.flush();
        }
        else
            updateAccessTimeWithTransaction(inodeid, aTime);
    }
    
    /**Updates the access time of an inode in the database
     * @param inodeid
     * @param modTime
     */
    private static void updateAccessTimeWithTransaction(long inodeid, long aTime) {
      boolean done = false;
      int tries = RETRY_COUNT;

      Session session = DBConnector.obtainSession();
      Transaction tx = session.currentTransaction();
      while (done == false && tries > 0) {
        try {
          DBConnector.beginTransaction();
          updateAccessTimeInternal(session, inodeid, aTime);
          DBConnector.commit();
          done = true;
          //session.flush();
        } catch (ClusterJException e) {
          if (tx.isActive()) {
            DBConnector.safeRollback();
          }
          e.printStackTrace();
          tries--;
        }
      }

    }

    /**Internal function for updating the access time of an inode in the database
     * @param session
     * @param inodeid
     * @param modTime
     */
    private static void updateAccessTimeInternal(Session session, long inodeid, long aTime) throws ClusterJException{
            INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
            inodet.setATime(aTime);
            updateINodeTableInternal(session, inodet);
    }
    
    
    
    
    ////////////////////////////////
    // Fix for the lease recovery issue
    ///////////////////////////////
    

    public static void updateClientName(long inodeid, String newClientName, boolean isTransactional) throws ClusterJException
    {
      DBConnector.checkTransactionState(isTransactional);
      if (isTransactional) {
        Session session = DBConnector.obtainSession();
        updateClientNameInternal(session, inodeid, newClientName);
        session.flush();
      }
      else
        updateClientNameWithTransaction(inodeid, newClientName);
    }

    private static void updateClientNameWithTransaction(long inodeid,
        String newClientName) {
      boolean done = false;
      int tries = RETRY_COUNT;

      Session session = DBConnector.obtainSession();
      Transaction tx = session.currentTransaction();
      while (done == false && tries > 0) {
        try {
          DBConnector.beginTransaction();
          updateClientNameInternal(session, inodeid, newClientName);
          DBConnector.commit();
          done = true;
          session.flush();
        } catch (ClusterJException e) {
          if (tx.isActive()) {
            DBConnector.safeRollback();
          }
          e.printStackTrace();
          tries--;
        }
      }
    }

    private static void updateClientNameInternal(Session session, long inodeid,
        String newClientName) {
      INodeTableSimple inodet = session.newInstance(INodeTableSimple.class, inodeid);
      inodet.setClientName(newClientName);
      updateINodeTableInternal(session, inodet);
    }
    

}