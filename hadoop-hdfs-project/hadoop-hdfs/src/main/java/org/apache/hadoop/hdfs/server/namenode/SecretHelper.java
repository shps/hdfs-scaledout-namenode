/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import se.sics.clusterj.DelegationKeyTable;
import se.sics.clusterj.INodeTableSimple;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.hadoop.hdfs.server.namenode.metrics.HelperMetrics;

/**
 * @author wmalik
 *
 */
public class SecretHelper {
	
	private static final Log LOG = LogFactory.getLog(SecretHelper.class);
	private static final int RETRY_COUNT = 3;
	
	public static final short CURR_KEY = 0;
	public static final short NEXT_KEY = 1;
	public static final short SIMPLE_KEY = -1;
	
	/** Puts a blockKey in the database (handles the serialization of blockKey)
	 * @param keyId
	 * @param blockKey
	 */
	public static void put(int keyId, BlockKey blockKey, short KEY_TYPE) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();

		DataOutputBuffer keyBytes = new DataOutputBuffer();
		try {
			blockKey.write(keyBytes);
		} catch (IOException e1) {
			LOG.error("IOException while converting blockKey to bytes: "
					+ e1.getMessage());
			e1.printStackTrace();
			return;
		}
		
		while(done == false && tries > 0) {
			try {	
				tx.begin();
				insert(session, keyId, blockKey.getExpiryDate(), keyBytes.getData(), KEY_TYPE);
				tx.commit();
				session.flush();
				done = true;
			}
			catch(ClusterJException e) {
				if(tx.isActive())
					tx.rollback();
				//LOG.error("ClusterJException in put(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
	}
	
	/** Gets a blockKey from the database (handles the serialization of blockKey)
	 * @param keyId
	 * @return
	 */
	public static BlockKey get(int keyId) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();

		while(done == false && tries > 0) {
			try {	
				DelegationKeyTable dkt = select(session, keyId);
				if(dkt == null)
					return null; //BlockTokenSecretManager.retrivePassword expects this
				BlockKey blockKey = convertToBlockKey(dkt);
				done = true;
				return blockKey;
			}
			catch(ClusterJException e) {
				//LOG.error("ClusterJException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			} catch (IOException e) {
				LOG.error("IOException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		
		return null;
	}
	
	/** Fetch all keys from database
	 * @return An array of BlockKey objects
	 */
	public static BlockKey[] getAllKeys() {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		while(done == false && tries > 0) {
			try {	
				List<DelegationKeyTable> dktList = selectAll(session);
				if(dktList == null || dktList.size() == 0)
					throw new NullPointerException("No key found in database");
				BlockKey[] blockKeys = new BlockKey[dktList.size()];
				for(int i=0;i<dktList.size(); i++) {
					blockKeys[i] = convertToBlockKey(dktList.get(i));
				}
				done = true;
				return blockKeys;
			}
			catch(ClusterJException e) {
				//LOG.error("ClusterJException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			} catch (IOException e) {
				LOG.error("IOException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		
		return null;
		
	}
	
	public static Map<Integer, BlockKey> getAllKeysMap() {
		Map<Integer, BlockKey> allKeys = new HashMap<Integer, BlockKey>();
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		while(done == false && tries > 0) {
			try {	
				List<DelegationKeyTable> dktList = selectAll(session);
				if(dktList == null || dktList.size() == 0)
					break;
				for(int i=0;i<dktList.size(); i++) {
					allKeys.put(dktList.get(i).getKeyId(), convertToBlockKey(dktList.get(i)));
				}
				done = true;
				return allKeys;
			}
			catch(ClusterJException e) {
				//LOG.error("ClusterJException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			} catch (IOException e) {
				LOG.error("IOException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		
		return new HashMap<Integer, BlockKey>();
	}
	
	
	/** Removes a key from the database
	 * @param keyId
	 */
	public static void removeKey(int keyId) {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		
		while(done == false && tries > 0) {
			try {	
				tx.begin();
				delete(session, keyId);
				tx.commit();
				session.flush();
				done = true;
				return;
			}
			catch(ClusterJException e) {
				if(tx.isActive())
					tx.rollback();
				//LOG.error(tries + " ClusterJException in removeKey(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		
		throw new RuntimeException("Unable to perform operation in NDB");
	}
	
	
	/** Fetches the current key from the database
	 * @return The current key
	 */
	public static BlockKey getCurrentKey() {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();

		while(done == false && tries > 0) {
			try {	
				DelegationKeyTable dkt = select(session, CURR_KEY);
				if(dkt == null)
					return null; //BlockTokenSecretManager.createPassword expects this
				BlockKey blockKey = convertToBlockKey(dkt);
				done = true;
				return blockKey;
			}
			catch(ClusterJException e) {
				//LOG.error("ClusterJException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			} catch (IOException e) {
				LOG.error("IOException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		
		return null;
	}
	
	/** Fetches the next key from the database
	 * @return the next key
	 */
	public static BlockKey getNextKey() {
		int tries = RETRY_COUNT;
		boolean done = false;
		Session session = DBConnector.obtainSession();

		while(done == false && tries > 0) {
			try {	
				DelegationKeyTable dkt = select(session, NEXT_KEY);
				BlockKey blockKey = convertToBlockKey(dkt);
				done = true;
				return blockKey;
			}
			catch(ClusterJException e) {
				//LOG.error("ClusterJException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			} catch (IOException e) {
				LOG.error("IOException in get(): " + e.getMessage());
				e.printStackTrace();
				tries--;
			}
		}
		
		return null;
	}
	
	
	/** Converts a DelegationKey tuple from NDB into a BlockKey
	 * @param dkt
	 * @return
	 * @throws IOException
	 */
	private static BlockKey convertToBlockKey(DelegationKeyTable dkt) throws IOException {
		byte[] keyBytes = dkt.getKeyBytes();
		DataInputStream dis =
		        new DataInputStream(new ByteArrayInputStream(keyBytes));
		BlockKey bKey = new BlockKey();
		bKey.readFields(dis);
		return bKey;
	}
	
	
	//////////////////////////
	// Database functions
	//////////////////////////
	
	/** Primary key lookup using keyId
	 * @param session
	 * @param keyId
	 * @return a row from DelegationKey table if it exists, and null otherwise
	 */
	private static DelegationKeyTable select(Session session, int keyId){
    HelperMetrics.secretMetrics.incrSelectUsingPKey();
    
		return session.find(DelegationKeyTable.class, keyId);
	}
	
	private static DelegationKeyTable select(Session session, short KEY_TYPE){
    
    HelperMetrics.secretMetrics.incrSelectUsingIndex();
    
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<DelegationKeyTable> dobj = qb.createQueryDefinition(DelegationKeyTable.class);
		PredicateOperand field = dobj.get("keyType");
		Predicate predicate = field.equal(dobj.param("param1"));
		dobj.where(predicate);
		Query<DelegationKeyTable> query = session.createQuery(dobj);
		query.setParameter("param1", KEY_TYPE);
		List<DelegationKeyTable> results = query.getResultList();
		if(results == null || results.size() == 0)
			return null;
		else if (results.size() > 1) 
			throw new RuntimeException("More than 1 keys found for KeyType " 
									+ KEY_TYPE + " - This should never happen or the world will end" );
		else {
			return results.get(0);
		}
	}
	
	/** Fetches all the rows from DelegationKey table
	 * @param session
	 * @return all rows from the DelegationKeyTable
	 */
	private static List<DelegationKeyTable> selectAll(Session session){
    HelperMetrics.secretMetrics.incrSelectAll();
    
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<DelegationKeyTable> dobj = qb.createQueryDefinition(DelegationKeyTable.class);
		Query<DelegationKeyTable> query = session.createQuery(dobj);
		return 	query.getResultList();
	}
	
	/** Insert a new row in DelegationKey table. If the row with the same key
	 * exists in NDB, it will be updated. If a row with <param>keyType</param>
	 * exists in NDB, it will deleted and a new row will be inserted
	 * @param session
	 * @param keyId
	 * @param expiryDate
	 * @param keyBytes
	 */
	private static void insert(Session session, int keyId, long expiryDate, byte[] keyBytes, short keyType){
		if(keyType == CURR_KEY || keyType == NEXT_KEY) {
			DelegationKeyTable dkte = select(session, keyType);
			if(dkte != null) {
				delete(session,dkte.getKeyId());
			}
		}
		
    HelperMetrics.secretMetrics.incrInsert();
    
		DelegationKeyTable dkt = session.newInstance(DelegationKeyTable.class, keyId);
		dkt.setExpiryDate(expiryDate);
		dkt.setKeyBytes(keyBytes);
		dkt.setKeyType(keyType);
		session.savePersistent(dkt); //using save to imitate the functionality of Map.put()
	}
	
	/** Updates an already existing row in DelegationKey table
	 * @param session
	 * @param dkt
	 */
	private static void update(Session session, DelegationKeyTable dkt){
    HelperMetrics.secretMetrics.incrUpdate();
    
		session.updatePersistent(dkt);
	}
	
	/** Delete a key from the DelegationKey table
	 * @param session
	 * @param keyId
	 */
	private static void delete(Session session, int keyId){
    HelperMetrics.secretMetrics.incrDelete();
    
		session.deletePersistent(DelegationKeyTable.class, keyId);
	}
	
	/** Delete all rows from the DelegationKey table
	 * @param session
	 */
	private static void deleteAll(Session session){
    HelperMetrics.secretMetrics.incrDelete();
    
		session.deletePersistentAll(DelegationKeyTable.class);
	}

}
