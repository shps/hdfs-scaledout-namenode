package org.apache.hadoop.hdfs.server.namenode;


import se.sics.clusterj.BlockTotalTable;
import com.mysql.clusterj.ClusterJException;
import se.sics.clusterj.BlockInfoTable;
import se.sics.clusterj.INodeTableSimple;
import se.sics.clusterj.LeasePathsTable;
import se.sics.clusterj.LeaseTable;
import se.sics.clusterj.TripletsTable;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_CONNECTOR_STRING_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_CONNECTOR_STRING_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_DATABASE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_DATABASE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_NUM_SESSION_FACTORIES;


/* 
 * This singleton class serves sessions to the Inode/Block
 * helper classes to talk to the DB. 
 * 
 * Three design decisions here:
 * 1) Serve one ClusterJ Session per Namenode
 *    worker thread, because Sessions are not thread
 *    safe.
 * 2) Have a pool of ClusterJ SessionFactory instances
 *    to serve the Sessions. This will help work around
 *    contention at the ClusterJ internal buffers.
 * 3) Set the connection pool size to be as many as
 *    the number of SessionFactory instances. This will
 *    allow multiple simultaneous connections to exist,
 *    and the read/write locks in FSNamesystem and 
 *    FSDirectory will make sure this stays safe. * 
 */
public class DBConnector {
	private static int NUM_SESSION_FACTORIES;
	static SessionFactory [] sessionFactory;
	static Map<Long, Session> sessionPool = new ConcurrentHashMap<Long, Session>();
        public static final int RETRY_COUNT = 3;
	
	public static void setConfiguration (Configuration conf){
		NUM_SESSION_FACTORIES = conf.getInt(DFS_DB_NUM_SESSION_FACTORIES, 3);
		sessionFactory = new SessionFactory[NUM_SESSION_FACTORIES];
		
		for (int i = 0; i < NUM_SESSION_FACTORIES; i++)
		{
			Properties p = new Properties();
			p.setProperty("com.mysql.clusterj.connectstring", conf.get(DFS_DB_CONNECTOR_STRING_KEY, DFS_DB_CONNECTOR_STRING_DEFAULT));
			p.setProperty("com.mysql.clusterj.database", conf.get(DFS_DB_DATABASE_KEY, DFS_DB_DATABASE_DEFAULT));
			p.setProperty("com.mysql.clusterj.connection.pool.size", String.valueOf(NUM_SESSION_FACTORIES));
			sessionFactory[i] = ClusterJHelper.getSessionFactory(p);
		}
	}
	
	/*
	 * Return a session from a random session factory in our
	 * pool.
	 * 
	 * NOTE: Do not close the session returned by this call
	 * or you will die.
	 */
	public synchronized static Session obtainSession (){
		long threadId = Thread.currentThread().getId();
		
		if (sessionPool.containsKey(threadId))	{
			return sessionPool.get(threadId); 
		}
		else {
			// Pick a random sessionFactory
			Random r = new Random();
			System.err.println("NUM_SESS_FACTS: " + NUM_SESSION_FACTORIES);
			Session session = sessionFactory[r.nextInt(NUM_SESSION_FACTORIES)].getSession();
			sessionPool.put(threadId, session);
			return session;
		}
	}
        
        /**
         * begin a transaction.
         */
        public static void beginTransaction() throws ClusterJUserException
        {
            Session session = obtainSession();
            Transaction tx = session.currentTransaction();
            if(tx.isActive())
            {
                    throw new ClusterJUserException("Transaction is already active");
            }
            else
            {
                    tx.begin();
            }
                            
        }
        
        /**
         * Commit a transaction.
         */
        public static void commit() throws ClusterJUserException
        {
            Session session = obtainSession();
            Transaction tx = session.currentTransaction();
            if (!tx.isActive())
                throw new ClusterJUserException("The transaction is not started!");
            
            tx.commit();
            // session.flush(); why?
        }
        
        /**
         * It rolls back only when the transaction is active.
         */
        public static void safeRollback() throws ClusterJUserException
        {
            Session session = obtainSession();
            Transaction tx = session.currentTransaction();
            if (tx.isActive())
                tx.rollback();
        }
        /**
         * Returns the current Transaction.
         */
        public static Transaction getTransaction() throws ClusterJUserException
        {
            Session session = obtainSession();
            return session.currentTransaction();
        }
        
        /**
        
         * This is called only when MiniDFSCluster wants to format the Namenode.
        
         */
        public static boolean formatDB()
        {

                Session session = obtainSession();

                Transaction tx = session.currentTransaction();

                try
                {

                        tx.begin();

                        session.deletePersistentAll(INodeTableSimple.class);

                        session.deletePersistentAll(BlockInfoTable.class);

                        session.deletePersistentAll(LeaseTable.class);

                        session.deletePersistentAll(LeasePathsTable.class);

                        session.deletePersistentAll(TripletsTable.class);
                        
                        // KTHFS: Added 'true' for isTransactional. Later needs to be changed when we add the begin and commit tran clause
                        session.deletePersistentAll(BlockTotalTable.class);
                        BlocksHelper.resetTotalBlocks(true);

                        tx.commit();

                        session.flush();

                        return true;

                }
                catch (ClusterJException ex)
                {

                        //LOG.error(ex.getMessage(), ex);
                        System.err.println(ex.getMessage());
                        ex.printStackTrace();
                        
                        tx.rollback();

                }

                return false;

        }        
}
