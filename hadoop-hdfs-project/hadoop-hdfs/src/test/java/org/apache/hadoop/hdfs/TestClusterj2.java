package org.apache.hadoop.hdfs;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
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
public class TestClusterj2 {

  static SessionFactory sessionFactory;
  static HdfsConfiguration conf;
  final static Log LOG = LogFactory.getLog(TestRowLevelLock.class);
  final int size = 5000;
  final int numThreads = 100;
  final int opsPerThread = size / numThreads;
  final String[][] userNames = new String[numThreads][opsPerThread];
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
  public void testReadCommitForUsers() throws InterruptedException {
    buildUserDataStructure(userNames);

    long rcTime = 0;
    Thread[] threads = new Thread[numThreads];
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    for (int i = 0; i < threads.length; i++) {
      Runnable rcReader = buildAReadCommitedReaderForUsers(barrier, latch, userNames[i]);
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

  private Runnable buildAReadCommitedReaderForUsers(final CyclicBarrier barrier, final CountDownLatch latch, final String[] userNames) {
    Runnable rcReader = new Runnable() {

      @Override
      public void run() {
        try {
          LOG.fatal("Thread " + sid + " is starting...");
          barrier.await();
          Session session = sessionFactory.getSession();
          session.setLockMode(LockMode.READ_COMMITTED);
          for (int i = 0; i < userNames.length; i++) {
            session.currentTransaction().begin();
            UserDTO user = readUser(userNames[i], session);
            assert user != null && user.getName().equals(userNames[i]);
//            if (i + 1 < userNames.length) {
//              UserDTO user2 = readUser(userNames[i + 1], session);
//              assert user2 != null && user2.getName().equals(userNames[i + 1]);
//            }
//            if (i + 2 < userNames.length) {
//              UserDTO user3 = readUser(userNames[i + 2], session);
//              assert user3 != null && user3.getName().equals(userNames[i + 2]);
//            }
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

  private UserDTO readUser(String name, Session session) {
    QueryBuilder qb = session.getQueryBuilder();

    QueryDomainType<UserDTO> dobj = qb.createQueryDefinition(UserDTO.class);

    Predicate pred1 = dobj.get("name").equal(dobj.param("name"));
    Predicate pred2 = dobj.get("sId").equal(dobj.param("sId"));

    dobj.where(pred1.and(pred2));
    Query<UserDTO> query = session.createQuery(dobj);

    query.setParameter(
            "name", name);
    query.setParameter(
            "sId", sid);
    List<UserDTO> results = query.getResultList();

    if (results.size() > 1) {
      throw new RuntimeException("This parent has two chidlren with the same name");
    } else if (results.isEmpty()) {
      return null;
    } else {
      return results.get(0);
    }
  }

  private void buildUserDataStructure(String[][] userNames) {
    Random rand = new Random();
    String prefix = "t";
    LinkedList<UserDTO> users = new LinkedList<UserDTO>();
    Session session = sessionFactory.getSession();
    formatDatabase(session);
//    int userId = 1;
    for (int i = 0; i < userNames.length; i++) {
      String threadFiles[] = userNames[i];
      for (int j = 0; j < threadFiles.length; j++) {
        threadFiles[j] = i + prefix + j;
        users.add(createUser(rand.nextLong(), threadFiles[j], session));
      }
    }

    System.out.println("Building users data...");
    Transaction tx = session.currentTransaction();
    tx.begin();
    session.savePersistentAll(users);
    session.flush();
    tx.commit();
  }

  private UserDTO createUser(long id, String name, Session session) {
    UserDTO user = session.newInstance(UserDTO.class);
    user.setName(name);
    user.setId(id);
    user.setSId(sid);
    return user;
  }

  private void formatDatabase(Session session) {
    session.deletePersistentAll(InodeDTO.class);
    session.deletePersistentAll(UserDTO.class);
    session.flush();
  }

  @PersistenceCapable(table = "users")
  public interface UserDTO {

    @PrimaryKey
    @Column(name = "id")
    long getId();     // id of the inode

    void setId(long id);

    @Column(name = "s_id")
    @Index(name = "path_lookup_idx")
    long getSId();     // id of the inode

    void setSId(long id);

    @Column(name = "name")
    @Index(name = "path_lookup_idx")
    String getName();     //name of the inode

    void setName(String name);

    ///////////////
    // marker for InodeDirectory
    @Column(name = "is_dir")
    boolean getIsDir();

    void setIsDir(boolean isDir);

    // marker for InodeDirectoryWithQuota
    @Column(name = "is_dir_with_quota")
    boolean getIsDirWithQuota();

    void setIsDirWithQuota(boolean isDirWithQuota);

    // Inode
    @Column(name = "modification_time")
    long getModificationTime();

    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = "access_time")
    long getATime();

    void setATime(long modificationTime);

    
    @Column(name = "permission")
    byte[] getPermission();

    void setPermission(byte[] permission);

    // InodeDirectoryWithQuota
    @Column(name = "nscount")
    long getNSCount();

    void setNSCount(long nsCount);

    // InodeDirectoryWithQuota
    @Column(name = "dscount")
    long getDSCount();

    void setDSCount(long dsCount);

    // InodeDirectoryWithQuota
    @Column(name = "nsquota")
    long getNSQuota();

    void setNSQuota(long nsQuota);

    // InodeDirectoryWithQuota
    @Column(name = "dsquota")
    long getDSQuota();

    void setDSQuota(long dsQuota);

    //  marker for InodeFileUnderConstruction
    @Column(name = "is_under_construction")
    boolean getIsUnderConstruction();

    void setIsUnderConstruction(boolean isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = "client_name")
    String getClientName();

    void setClientName(String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = "client_machine")
    String getClientMachine();

    void setClientMachine(String clientMachine);

    @Column(name = "client_node")
    String getClientNode();

    void setClientNode(String clientNode);

    //  marker for InodeFile
    @Column(name = "is_closed_file")
    boolean getIsClosedFile();

    void setIsClosedFile(boolean isClosedFile);

    // InodeFile
    @Column(name = "header")
    long getHeader();

    void setHeader(long header);

    //INodeSymlink
    @Column(name = "symlink")
    String getSymlink();

    void setSymlink(String symlink);
  }
}
