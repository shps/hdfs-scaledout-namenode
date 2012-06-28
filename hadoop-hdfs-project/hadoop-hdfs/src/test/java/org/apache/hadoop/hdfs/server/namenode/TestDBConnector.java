package org.apache.hadoop.hdfs.server.namenode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
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
    StorageConnector connector = StorageFactory.getConnector();
    connector.setConfiguration(conf);
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
      assert t.connector.obtainSession() != null;
      assert t.connector.isTransactionActive();
    }

    LOG.info(numOfSessions + " parallel sessions are open now.");

    for (DBConnectorTest t : threads) {
      t.connector.rollback();
    }
  }

  private class DBConnectorTest extends Thread {

    CountDownLatch cdl;
    public StorageConnector connector = StorageFactory.getConnector();

    public DBConnectorTest(CountDownLatch cdl) {
      this.cdl = cdl;
    }

    @Override
    public void run() {
      LOG.info("Thread " + getId() + " obtained the session.");
      connector.beginTransaction();
      LOG.info("Thread " + getId() + " beginned the transaction.");
      cdl.countDown();
    }
  }
}
