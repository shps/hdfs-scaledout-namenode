/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import se.sics.clusterj.DelegationKeyTable;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

/**
 * @author wmalik
 *
 */
class SecretHelper {
	
	private static final Log LOG = LogFactory.getLog(SecretHelper.class);
	private static final int RETRY_COUNT = 3;
	
	/** Puts a blockKey in the database (handles the serialization of blockKey)
	 * @param keyId
	 * @param blockKey
	 */
	public static void put(int keyId, BlockKey blockKey) {
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
				insert(session, keyId, blockKey.getExpiryDate(), keyBytes.getData(), (short)1337);
				tx.commit();
				session.flush();
				done = true;
			}
			catch(ClusterJException e) {
				if(tx.isActive())
					tx.rollback();
				LOG.error("ClusterJException in put(): " + e.getMessage());
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
				BlockKey blockKey = convertToBlockKey(dkt);
				done = true;
				return blockKey;
			}
			catch(ClusterJException e) {
				LOG.error("ClusterJException in get(): " + e.getMessage());
				tries--;
			} catch (IOException e) {
				LOG.error("IOException in get(): " + e.getMessage());
				e.printStackTrace();
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
		DataInputBuffer keyBytes = new DataInputBuffer();
		keyBytes.read(dkt.getKeyBytes());
		BlockKey bKey = new BlockKey();
		bKey.readFields(keyBytes);
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
		return session.find(DelegationKeyTable.class, keyId);
	}
	
	/** Fetches all the rows from DelegationKey table
	 * @param session
	 * @return all rows from the DelegationKeyTable
	 */
	private static List<DelegationKeyTable> selectAll(Session session){
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<DelegationKeyTable> dobj = qb.createQueryDefinition(DelegationKeyTable.class);
		Query<DelegationKeyTable> query = session.createQuery(dobj);
		return 	query.getResultList();
	}
	
	/** Insert a new row in DelegationKey table. If the row already
	 *  exists, it will be updated
	 * @param session
	 * @param keyId
	 * @param expiryDate
	 * @param keyBytes
	 */
	private static void insert(Session session, int keyId, long expiryDate, byte[] keyBytes, short keyType){
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
		session.updatePersistent(dkt);
	}
	
	/** Delete a key from the DelegationKey table
	 * @param session
	 * @param keyId
	 */
	private static void delete(Session session, int keyId){
		session.deletePersistent(DelegationKeyTable.class, keyId);
	}
	
	/** Delete all rows from the DelegationKey table
	 * @param session
	 */
	private static void deleteAll(Session session){
		session.deletePersistentAll(DelegationKeyTable.class);
	}

}
