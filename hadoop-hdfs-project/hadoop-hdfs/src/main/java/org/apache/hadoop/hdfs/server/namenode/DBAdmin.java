/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


import se.sics.clusterj.BlockInfoTable;
import se.sics.clusterj.DelegationKeyTable;
import se.sics.clusterj.INodeTableSimple;
import se.sics.clusterj.LeasePathsTable;
import se.sics.clusterj.LeaseTable;
import se.sics.clusterj.TripletsTable;

import com.mysql.clusterj.Session;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**Used for performing administrative functions on the database. 
 * @author wmalik
 */
public class DBAdmin {
 static final Log LOG = LogFactory.getLog(DBAdmin.class);
	
	/** Deletes all rows from all kthfs tables
	 * @param session
	 * @param database
	 */
	public static void deleteAllTables(Session session, String database) {
		session.deletePersistentAll(DelegationKeyTable.class);
		session.deletePersistentAll(INodeTableSimple.class);
		session.deletePersistentAll(LeaseTable.class);
		session.deletePersistentAll(LeasePathsTable.class);
		session.deletePersistentAll(TripletsTable.class);
		session.deletePersistentAll(BlockInfoTable.class);
		
	}
	
}
