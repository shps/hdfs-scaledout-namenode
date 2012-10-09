package org.apache.hadoop.hdfs;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.InodeClusterj.InodeDTO;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TestClusterj {

  static SessionFactory sessionFactory;
  static HdfsConfiguration conf;
  InodeDTO root;
  InodeDTO parent;
  final static Log LOG = LogFactory.getLog(TestRowLevelLock.class);
  final int size = 5000;
  final int numThreads = 100;
  final int opsPerThread = size / numThreads;
  final String[][] files = new String[numThreads][opsPerThread];
  final static long sid = 0;

  @BeforeClass
  public static void setupConnection() {
    conf = new HdfsConfiguration();
    Properties p = new Properties();
    p.setProperty(Constants.PROPERTY_CLUSTER_CONNECTSTRING, conf.get(DFSConfigKeys.DFS_DB_CONNECTOR_STRING_KEY));
    p.setProperty(Constants.PROPERTY_CLUSTER_DATABASE, conf.get(DFSConfigKeys.DFS_DB_DATABASE_KEY));
    p.setProperty(Constants.PROPERTY_CONNECTION_POOL_SIZE, conf.get(DFSConfigKeys.DFS_DB_NUM_SESSION_FACTORIES));
    p.setProperty(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS, "1024");
    sessionFactory = ClusterJHelper.getSessionFactory(p);
  }

  @Before
  public void init() {
//    
    
  }

  @Test
  public void testReadCommitForInodes() throws InterruptedException {
    buildInodeDataStructures(files);
    long rcTime = 0;
    Thread[] threads = new Thread[numThreads];
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    for (int i = 0; i < threads.length; i++) {
      Runnable rcReader = buildAReadCommitedReaderForInodes(root, parent, barrier, latch, files[i]);
      threads[i] = new Thread(rcReader);
    }
    LOG.fatal("Reading data by read-commit...");
    rcTime = measureReadTime(threads, latch);
    LOG.fatal(String.format("Read-Commit time: %d", rcTime));
  }

  private long measureReadTime(Thread[] threads, CountDownLatch latch) throws InterruptedException {
    long start = System.currentTimeMillis();
    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }
    latch.await();
    long runTime = System.currentTimeMillis() - start;
    threads = null;
    latch = null;
    System.gc();
    return runTime;
  }

  private Runnable buildAReadCommitedReaderForInodes(final InodeDTO root, final InodeDTO parent,
          final CyclicBarrier barrier, final CountDownLatch latch, final String[] files) {
    Runnable rcReader = new Runnable() {

      @Override
      public void run() {
        try {
//          LOG.fatal("Thread " + Thread.currentThread().getName() + " is starting...");
          barrier.await();
          Session session = sessionFactory.getSession();
          session.setLockMode(LockMode.READ_COMMITTED);
          for (int i = 0; i < files.length; i++) {
            session.currentTransaction().begin();

//            InodeDTO readParent = readParent(root, parent, session);

            InodeDTO readFile = readINode(0, files[i], session);
            assert readFile != null && readFile.getName().equals(files[i]);
            session.currentTransaction().commit();
            Thread.sleep(10);
          }
          latch.countDown();
        } catch (InterruptedException ex) {
          Logger.getLogger(TestTransactionalOperations.class.getName()).log(Level.SEVERE, null, ex);
          assert false : ex.getMessage();
        } catch (BrokenBarrierException ex) {
          Logger.getLogger(TestTransactionalOperations.class.getName()).log(Level.SEVERE, null, ex);
          assert false : ex.getMessage();
        }
      }
    };
    return rcReader;
  }

  private void buildInodeDataStructures(String[][] files) {
    Random rand = new Random();
    String prefix = "t";
    LinkedList<InodeDTO> inodes = new LinkedList<InodeDTO>();
    Session session = sessionFactory.getSession();
    formatDatabase(session);
//    root = createInode(0L, "root", -1L, session);
//    inodes.add(root);
//    int inodeId = 1;
//    long pid = root.getId();
//    parent = createInode(inodeId, "parent", pid, session);
//    inodes.add(parent);
    for (int i = 0; i < files.length; i++) {
      String threadFiles[] = files[i];
      for (int j = 0; j < threadFiles.length; j++) {
        threadFiles[j] = i + prefix + j;
        inodes.add(createInode(rand.nextLong(), threadFiles[j], 0, session));
      }
    }

    System.out.println("Building the data...");
    Transaction tx = session.currentTransaction();
    tx.begin();
    session.savePersistentAll(inodes);
    session.flush();
    tx.commit();
  }
  private InodeDTO createInode(long id, String name, long pid, Session session) {
    InodeDTO inode = session.newInstance(InodeDTO.class);
    inode.setParentId(pid);
    inode.setName(name);
    inode.setId(id);
    return inode;
  }

  private void formatDatabase(Session session) {
    session.deletePersistentAll(InodeDTO.class);
    session.flush();
  }

  private InodeDTO readINode(long parentId, String name, Session session) {
    QueryBuilder qb = session.getQueryBuilder();

    QueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);

    Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
    Predicate pred2 = dobj.get("parentId").equal(dobj.param("parentID"));

    dobj.where(pred1.and(pred2));
    Query<InodeDTO> query = session.createQuery(dobj);

    query.setParameter(
            "name", name);
    query.setParameter(
            "parentID", parentId);
    List<InodeDTO> results = query.getResultList();

    if (results.size() > 1) {
      throw new RuntimeException("This parent has two chidlren with the same name");
    } else if (results.isEmpty()) {
      return null;
    } else {
      return results.get(0);
    }
  }
  private InodeDTO readParent(InodeDTO root, InodeDTO parent, Session session) {
    InodeDTO readRoot = readINode(root.getParentId(), root.getName(), session);
    assert readRoot.getId() == root.getId();
    long pid = readRoot.getId();
    InodeDTO readParent = readINode(pid, parent.getName(), session);
    assert readParent.getId() == parent.getId();
    return readParent;
  }
}
