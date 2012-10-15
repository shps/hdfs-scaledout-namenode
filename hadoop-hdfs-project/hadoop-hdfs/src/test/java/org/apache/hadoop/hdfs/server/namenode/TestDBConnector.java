
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.persistance.DBConnector;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TestDBConnector {

    public static final Log LOG = LogFactory.getLog(TestDBConnector.class.getName());

    @BeforeClass
    public static void initialize() {
        Configuration conf = new HdfsConfiguration();
        DBConnector.setConfiguration(conf);
    }

    /**
     * Checks if 500 parallel sessions can be obtained and begin transaction.
     */
    @Test
    public void testNumberOfParallelSession() {
        int numOfSessions = 500;
        DBConnectorTest[] threads = new DBConnectorTest[numOfSessions];
        CountDownLatch cdl = new CountDownLatch(numOfSessions);

        for (int i = 0; i < numOfSessions; i++) {
            threads[i] = new DBConnectorTest(cdl);
            threads[i].start();
        }

        LOG.info("Main thread starts waiting for the sessions to be obtained.");
        try {
            cdl.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            LOG.warn(ex.getMessage(), ex);
        }

        long remaining = cdl.getCount();
        assert remaining == 0 : "There are " + remaining + " sessions left to be obtained.";

        LOG.info("Check if " + numOfSessions + " parallel sessions are open.");

        for (int i = 0; i < numOfSessions; i++) {
            LOG.info("Check session number " + i);
            DBConnectorTest t = threads[i];
            assert t.s != null;
            Transaction tx = t.s.currentTransaction();
            assert tx.isActive();
        }

        LOG.info(numOfSessions + " parallel sessions are open now.");

        for (DBConnectorTest t : threads) {
            Transaction tx = t.s.currentTransaction();
            tx.rollback();
        }
    }

    private class DBConnectorTest extends Thread {

        CountDownLatch cdl;
        public Session s;

        public DBConnectorTest(CountDownLatch cdl) {
            this.cdl = cdl;
        }

        @Override
        public void run() {
            this.s = DBConnector.obtainSession();
            LOG.info("Thread " + getId() + " obtained the session.");
            s.currentTransaction().begin();
            LOG.info("Thread " + getId() + " beginned the transaction.");
            cdl.countDown();
        }
    }
}
