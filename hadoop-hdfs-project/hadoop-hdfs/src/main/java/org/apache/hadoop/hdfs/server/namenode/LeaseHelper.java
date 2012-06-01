package org.apache.hadoop.hdfs.server.namenode;




import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;

import se.sics.clusterj.*;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.hadoop.hdfs.server.namenode.metrics.HelperMetrics;
import org.apache.hadoop.hdfs.server.namenode.metrics.LeaseMetrics;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import static org.apache.hadoop.hdfs.server.common.Util.now;


/** This is a helper class for manipulating the Leases stored in DB. 
 *  All methods ending with "Internal" in this class must be wrapped with DBConnector.beginTransaction() and DBConnector.commit(). 
 *  This gives us an opportunity to pack multiple operations in a single transaction to reduce round-trips
 * 
 *  @author wmalik
 */
public class LeaseHelper {

	private static final Log LOG = LogFactory.getLog(LeaseHelper.class);
	private static LeaseManager lm = null;
	private static final int RETRY_COUNT = 3; 


	/**
	 * This method creates a Lease object using the values stored in leaseTable and pathList
	 * @param leaseTable
	 * @param pathList
	 * @return a full cooked Lease object ready to be utilized by HDFS code
	 * @throws NullPointerException
	 */
	private static Lease createLeaseObject(LeaseTable leaseTable) {

		if (leaseTable == null)
			return null;
		if (lm == null) {
			LOG.error("LeaseManager has not been initialized yet");
			return null;
		}
		Lease lease = lm.new Lease(leaseTable.getHolder(), leaseTable.getHolderID(), leaseTable.getLastUpdated());
//		TreeSet<String> paths = convertLeasePathsTableToTreeSet(pathList);	
//		lease.setPaths(paths);
		return lease;
	}

        /**
	 * Adds a lease to the database with a path. This method should ONLY be called if the lease does not exist in database already
	 * Roundtrips: 3
	 * @param holder
	 * @param src
	 * @param lastUpd
	 * @return an updated Lease object. It will only return null if the database is down.
	 */
	public static Lease addLease(String holder, int holderID, String src, long lastUpd,
                boolean isTransactional) {
		DBConnector.checkTransactionState(isTransactional);
                Lease lease = null;
                
                if (isTransactional)
                {
                    Session session = DBConnector.obtainSession();
                    insertLeaseInternal(session, holder, lastUpd, holderID);
//                    insertLeasePathsInternal(session, holderID, src);
                    session.flush();
                    lease = LeaseHelper.getLease(holder);
                }
                else
                    lease = addLeaseWithTransaction(holder, holderID, src, lastUpd);
                
                return lease;
	}
        
	/**
	 * Checks to see if lease exists
	 * @param holder
	 * @return boolean true: if exists and false: if it doesn't
	 */
	public static boolean exists(String holder) {
		int tries=RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();

		while (done == false && tries > 0 ){
			try {
				LeaseTable leaseTable = selectLeaseTableInternal(session, holder);
				done = true;
				if(leaseTable == null)
				{
					return false;
				}
				else
				{
					return true;
				}
				//return leaseTable != null ? true : false;
			}
			catch (ClusterJException e){
				LeaseHelper.LOG.error("ClusterJException in getPaths: " + e.getMessage());
				tries--;
			}
		}

		return false; 
	}

	/**
	 * Adds a lease to the database with a path. This method should ONLY be called if the lease does not exist in database already
	 * Roundtrips: 3
	 * @param holder
	 * @param src
	 * @param lastUpd
	 * @return an updated Lease object. It will only return null if the database is down.
	 */
	public static Lease addLeaseWithTransaction(String holder, int holderID, String src, long lastUpd) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while(done == false && tries > 0) {
			try {	
				DBConnector.beginTransaction();
				LeaseHelper.insertLeaseInternal(session, holder, lastUpd, holderID);
//				LeaseHelper.insertLeasePathsInternal(session, holderID, src);
				DBConnector.commit();
				session.flush();

				Lease lease = LeaseHelper.getLease(holder);
				done = true;
				return lease;
			}
			catch(ClusterJException e) {
                                if (tx.isActive())
                                    DBConnector.safeRollback();
				LeaseHelper.LOG.error(e.getMessage(), e);
				tries--;
			}
		}

		return null;

	}

  /**Updates the lastUpdated time in database
   * Roundtrips: 2
   * @param holder
   */
  public static void renewLease(String holder, boolean isTransactional) {
    DBConnector.checkTransactionState(isTransactional);

    if (isTransactional) {
      Session session = DBConnector.obtainSession();
      LeaseHelper.renewLeaseInternal(session, holder);
      session.flush();
    } else {
      renewLeaseWithTransaction(holder);
    }
  }
        
	/**Updates the lastUpdated time in database
	 * Roundtrips: 2
	 * @param holder
	 */
	public static void renewLeaseWithTransaction(String holder) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while(done == false && tries > 0) {
			try{	
				DBConnector.beginTransaction();
				LeaseHelper.renewLeaseInternal(session, holder);
				DBConnector.commit();
                                done = true;
				session.flush();
			}
			catch(ClusterJException e) {
                                if (tx.isActive())
                                    DBConnector.safeRollback();
				LOG.error(e.getMessage(), e);
				tries--;
			}
		}
	}



	/**
	 * Fetches a lease from the database
	 * Roundtrips: 2
	 * @param holder
	 * @return a fully cooked Lease object ready to be utilized by HDFS, or null if lease holder does not exist
	 */
	public static Lease getLease(String holder) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		while(done == false && tries > 0) {
			try{	
				LeaseTable leaseTable = selectLeaseTableInternal(session, holder);
				if(leaseTable == null)
					return null;
//				List<LeasePathsTable> leasePathsTable = 
//						selectLeasePathsTableInternal(session, "holderID", leaseTable.getHolderID());
				Lease lease = createLeaseObject(leaseTable);
				done = true;
				return lease;

			} catch(ClusterJException e) {
				LeaseHelper.LOG.error(e.getMessage(), e);
				tries--;
			}
		}
		return null;
	}
        
        /**Renews an already existing Lease and adds a path to it
	 * @param holder the lease holder
	 * @param holderID 
	 * @param src the path of the file
	 * @return
	 */
	public static Lease renewLease(String holder, int holderID, String src,
                boolean isTransactional) {
		
                DBConnector.checkTransactionState(isTransactional);
                Lease lease = null;
                
                if (isTransactional)
                {
                    Session session = DBConnector.obtainSession();
                    renewLeaseInternal(session, holder);
//                    insertLeasePathsInternal(session, holderID, src);
                    session.flush();
                    lease = getLease(holder);
                }
                else
                    lease = renewLeaseWithTransaction(holder, holderID, src);
                
                return lease;
	}
 
	/**Renews an already existing Lease and adds a path to it
	 * @param holder the lease holder
	 * @param holderID 
	 * @param src the path of the file
	 * @return
	 */
	public static Lease renewLeaseWithTransaction(String holder, int holderID, String src) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		while(done == false && tries > 0) {
			try{	
				DBConnector.beginTransaction();
				LeaseHelper.renewLeaseInternal(session, holder);
//				LeaseHelper.insertLeasePathsInternal(session, holderID, src);
				done = true;
				DBConnector.commit();
				session.flush();
				//fetching a fresh Lease object from the database which reflects the new changes
				Lease lease = getLease(holder);
				return lease;

			}
			catch(ClusterJException e) {
                                if (tx.isActive()) //[Hooman]: This is necessary to be checked, because some exceptions close the transaction.
                                    DBConnector.safeRollback();
				LeaseHelper.LOG.error(e.getMessage(), e);
				tries--;
			}
		}
		return null;
	}

	private static List<LeaseTable> getAllLeaseTables(Session session) {
    HelperMetrics.leaseMetrics.incrSelectUsingAll();
    
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);
		Query<LeaseTable> query = session.createQuery(dobj);
		return 	query.getResultList();
	}

	/**
	 * @return A sorted set of leases
	 */
	public static SortedSet<Lease> getSortedLeases() {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();

		while(done == false && tries > 0) {
			try{
				SortedSet<Lease> sortedLeases = getSortedLeasesInternal(session);
				done = true;
				return sortedLeases;
			}
			catch(ClusterJException e) {
				LeaseHelper.LOG.error("ClusterJException in getSortedLeases: " + e.getMessage(), e);
				tries--;
			}
		}

		return new TreeSet<Lease>();
	}

	/**
	 * @return A map of leases sorted by path
	 */
	public static SortedMap<LeasePath, Lease> getSortedLeasesByPath() {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();

		while(done == false && tries > 0) {
			try{
				SortedMap<LeasePath, Lease> sortedLeases = getSortedLeasesByPathInternal(session);
				done = true;
				return sortedLeases;
			}
			catch(ClusterJException e) {
				//LeaseHelper.LOG.error("ClusterJException in getSortedLeases: " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}

		return new TreeMap<LeasePath, Lease>();
	}



	/**
	 * Searches for a lease by path
	 * @param src
	 * @return A fully cooked Lease object if it exists in database, otherwise returns null.
	 */
	public static Lease getLeaseByPath(String src) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();

		while(done == false && tries > 0) {
			try{
        EntityManager em = EntityManager.getInstance();
        LeasePath leasePath = em.findLeasePathByPath(src);
				if(leasePath != null) { 
					int holderID = leasePath.getHolderId();
					LeaseTable leaseTable = selectLeaseTableInternal(session, holderID);
					Lease lease = createLeaseObject(leaseTable);
					done = true;
					return lease;
				}
				else {
					done = true;
					return null;
				}
			}
			catch(ClusterJException e) {
				//LOG.error("ClusterJException in getLeaseByPath: " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}

		return null;
	}
	
        /**Delete a lease
	 * @param holder
	 */
	public static void deleteLease(String holder, boolean isTransactional) {
            DBConnector.checkTransactionState(isTransactional);
            
            if (isTransactional)
            {
                Session session = DBConnector.obtainSession();
                deleteLeaseInternal(session, holder);
                session.flush();
            }
            else
                deleteLeaseWithTransaction(holder);
	}
        
	/**Delete a lease
	 * @param holder
	 */
	private static void deleteLeaseWithTransaction(String holder) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		
		while(done == false && tries > 0) {
			try {
			DBConnector.beginTransaction();
			deleteLeaseInternal(session, holder);
			done = true;
			DBConnector.commit();
                        session.flush();
			} catch(ClusterJException e) {
                            LOG.error("ClusterJException in deleteLease: " + e.getMessage());
                            if (tx.isActive())
                                DBConnector.safeRollback();
                            tries--;
			}
		}
	}

	/**Sets the leaseManager object. This method should be called before using any of the helper functions in this class.
	 * @param leaseMgr
	 */
	public static void initialize(LeaseManager leaseMgr) {
		lm = leaseMgr;
	}


	////////////////////////////////////////////////////////////////////////
	// 				Internal Methods
	////////////////////////////////////////////////////////////////////////

	/**
	 * This method inserts a record (or updates if holder is already present) in the Lease table. 
	 * The call to this method should be wrapped in  DBConnector.beginTransaction() and DBConnector.commit() calls.
	 * 
	 * @param holder
	 * @param lastUpdated
	 */
	private static void insertLeaseInternal(Session session, String holder, long lastUpdated, int holderID) {

    HelperMetrics.leaseMetrics.incrInsert();
    
		LeaseTable leaseTable = session.newInstance(LeaseTable.class);
		leaseTable.setHolder(holder);
		leaseTable.setLastUpdated(lastUpdated);
		leaseTable.setHolderID(holderID);
		session.savePersistent(leaseTable);
	}

	/**
	 * This method is used for querying the Lease table
	 * @param session
	 * @param holder
	 * @return a LeaseTable object which is held by <b>holder</b>. If there is no holder, then it returns null
	 */
	private static LeaseTable selectLeaseTableInternal(Session session, String holder) {
    HelperMetrics.leaseMetrics.incrSelectUsingPKey();
    
		Object holderKey = holder;
		return session.find(LeaseTable.class, holderKey);
	}

	/**
	 * Deletes a Lease record from the database. After this method is executed, the deleteLeasePathsInternal
	 * method should be executed immediately
	 * 
	 * @param session
	 * @param holder
	 * @return the holderID of the deleted record, so that its 
	 * corresponding rows in LeasePaths can be deleted. Returns -1 if lease not found
	 */
	private static void deleteLeaseInternal(Session session, String holder) {		
    HelperMetrics.leaseMetrics.incrDelete();
    
		session.deletePersistent(LeaseTable.class, holder);
	}

	private static void renewLeaseInternal(Session session, String holder) {
		LeaseTable leaseTable = selectLeaseTableInternal(session, holder);
		leaseTable.setLastUpdated(now());
    
    HelperMetrics.leaseMetrics.incrUpdate();
    
		session.updatePersistent(leaseTable);
	}

	/**
	 * Returns a sorted set of leases. 
	 * @param session
	 * @return A SortedSet of Leases 
	 */
	private static SortedSet<Lease> getSortedLeasesInternal(Session session) {
		List<LeaseTable> leaseTables = getAllLeaseTables(session);
		SortedSet<Lease> sortedLeases = new TreeSet<Lease>();

		for(LeaseTable leaseTable : leaseTables) {
			sortedLeases.add(getLease(leaseTable.getHolder()));
		}
		return sortedLeases;
	}

	/**Fetches lease and paths from the database and returns a path sorted TreeMap<String, Lease>
	 * @param session
	 * @return TreeMap<String, Lease> sorted by path
	 */
	private static SortedMap<LeasePath, Lease> getSortedLeasesByPathInternal(Session session) {
		List<LeaseTable> leaseTables = getAllLeaseTables(session);
		SortedMap<LeasePath, Lease> sortedLeasesByPath = new TreeMap<LeasePath, Lease>();

		for(LeaseTable leaseTable : leaseTables) {
			//sortedLeases.add(getLease(leaseTable.getHolder()));
			Lease lease = getLease(leaseTable.getHolder());
			for(LeasePath lPath : lease.getPaths()) {
				sortedLeasesByPath.put(lPath, lease);
			}

		}
		return sortedLeasesByPath;
	}

	/**
	 * Selects rows in the Lease table using holderID. 
	 * If there is more than one row in the database with holderID, it throws an error and returns null.
	 * If no matching rows are found, it returns null.
	 * 
	 * This function causes a full table scan in the Lease table, so should be 
	 * used only if no other option is available.
	 *  
	 * @param session
	 * @param holderID
	 * @return a LeaseTable object, returns null if not found, or if two or more rows exist with same holderID
	 */
	private static LeaseTable selectLeaseTableInternal(Session session, int holderID) {
    HelperMetrics.leaseMetrics.incrSelectUsingPKey();
    
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<LeaseTable> dobj = qb.createQueryDefinition(LeaseTable.class);

		dobj.where(dobj.get("holderID").equal(dobj.param("param")));

		Query<LeaseTable> query = session.createQuery(dobj);
		query.setParameter("param", holderID); //the WHERE clause of SQL
		List<LeaseTable> leaseTables = query.getResultList();

		if(leaseTables.size() > 1) {
			LOG.error("Error in selectLeaseTableInternal: Multiple rows with same holderID");
			return null;
		}
		else if (leaseTables.size() == 1) {
			return leaseTables.get(0);
		}
		else {
			LOG.info("No rows found for holderID:" + holderID + " in Lease table");
			return null;
		}
	}

}