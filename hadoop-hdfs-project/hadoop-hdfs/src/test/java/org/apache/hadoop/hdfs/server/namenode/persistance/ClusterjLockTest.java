package org.apache.hadoop.hdfs.server.namenode.persistance;

import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ClusterjConnector;

/**
 *
 * @author kamal
 */
public class ClusterjLockTest {

    static Logger logger = Logger.getLogger(ClusterjLockTest.class.getName());

    @PersistenceCapable(table = "test_table1")
    public interface TestEntity1 {

        @PrimaryKey
        @Column(name = "entity_id")
        long getId();

        void setId(long id);
    }
    private ClusterjConnector connector = ClusterjConnector.INSTANCE;

    public void setup() {
        try {
            Configuration conf = new HdfsConfiguration();
            connector.setConfiguration(conf);
            connector.beginTransaction();
            Session session = connector.obtainSession();

            for (int i = 0; i < 4; i++) {
                TestEntity1 en = session.newInstance(TestEntity1.class);
                en.setId(i);
                session.persist(en);
            }

            session.flush();

            connector.commit();
        } catch (StorageException ex) {
            Logger.getLogger(ClusterjLockTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void cleanup() {
        connector.obtainSession().deletePersistentAll(TestEntity1.class);
    }
    public Thread worker1 = new Thread() {

        @Override
        public void run() {
            try {
                Session session = connector.obtainSession();

                connector.beginTransaction();

                session.setLockMode(LockMode.READ_COMMITTED);
                session.find(TestEntity1.class, new Long(0));
                logger.log(Level.INFO, "w1: rc(0)");


                session.setLockMode(LockMode.SHARED);
                session.find(TestEntity1.class, new Long(1));
                logger.log(Level.INFO, "w1: r(1)");

                session.setLockMode(LockMode.EXCLUSIVE);
                session.find(TestEntity1.class, new Long(2));
                logger.log(Level.INFO, "w1: w(2)");

                session.setLockMode(LockMode.SHARED);
                session.find(TestEntity1.class, new Long(3));
                logger.log(Level.INFO, "w1: r(3)");

                session.setLockMode(LockMode.EXCLUSIVE);
                session.find(TestEntity1.class, new Long(3));
                logger.log(Level.INFO, "w1: r->w(3)");

                logger.log(Level.INFO, "w1: .................................");
                worker1.sleep(5000);

                session.setLockMode(LockMode.SHARED);
                session.find(TestEntity1.class, new Long(2));
                logger.log(Level.INFO, "w1: w->r(2)");

                session.flush();
                
                logger.log(Level.INFO, "w1: .................................");
                worker1.sleep(5000);

                connector.commit();
                logger.log(Level.INFO, "w1: commit");

            } catch (StorageException ex) {
                Logger.getLogger(ClusterjLockTest.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(ClusterjLockTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    };
    public Thread worker2 = new Thread() {

        @Override
        public void run() {
            try {
                Session session = connector.obtainSession();

                connector.beginTransaction();

                session.setLockMode(LockMode.READ_COMMITTED);
                session.find(TestEntity1.class, new Long(0));
                logger.log(Level.INFO, "w2: rc(0)");

                session.setLockMode(LockMode.SHARED);
                session.find(TestEntity1.class, new Long(0));
                logger.log(Level.INFO, "w2: r(0)");

                session.setLockMode(LockMode.EXCLUSIVE);
                session.find(TestEntity1.class, new Long(0));
                logger.log(Level.INFO, "w2: w(0)");




                session.setLockMode(LockMode.READ_COMMITTED);
                session.find(TestEntity1.class, new Long(1));
                logger.log(Level.INFO, "w2: rc(1)");

                session.setLockMode(LockMode.SHARED);
                session.find(TestEntity1.class, new Long(1));
                logger.log(Level.INFO, "w2: r(1)");

                session.setLockMode(LockMode.READ_COMMITTED);
                session.find(TestEntity1.class, new Long(2));
                logger.log(Level.INFO, "w2: rc(2)");

                session.setLockMode(LockMode.SHARED);
                session.find(TestEntity1.class, new Long(2));
                logger.log(Level.INFO, "w2: r(2)");

                session.setLockMode(LockMode.EXCLUSIVE);
                session.find(TestEntity1.class, new Long(2));
                logger.log(Level.INFO, "w2: w(2)");

                connector.commit();
                logger.log(Level.INFO, "w2: commit");
            } catch (StorageException ex) {
                Logger.getLogger(ClusterjLockTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    };
    
    public Thread worker3 = new Thread() {

        @Override
        public void run() {
            try {
                Session session = connector.obtainSession();

                connector.beginTransaction();
                
                session.setLockMode(LockMode.SHARED);
                session.find(TestEntity1.class, new Long(3));
                logger.log(Level.INFO, "w3: r(3)");
                
                connector.commit();
                logger.log(Level.INFO, "w3: commit");
            } catch (StorageException ex) {
                Logger.getLogger(ClusterjLockTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    };

    public static void main(String[] args) {
        try {
            ClusterjLockTest test1 = new ClusterjLockTest();

            test1.setup();

            test1.worker1.start();

            Thread.currentThread().sleep(1000);

            test1.worker2.start();

            test1.worker3.start();
            
            test1.worker1.join();
            test1.worker2.join();
            test1.worker3.join();

            test1.cleanup();
        } catch (InterruptedException ex) {
            Logger.getLogger(ClusterjLockTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
