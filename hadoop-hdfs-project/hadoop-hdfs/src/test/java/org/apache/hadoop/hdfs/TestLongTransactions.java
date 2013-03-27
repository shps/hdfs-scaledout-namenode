package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InodeDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Salman <salman@sics.se>
 */
public class TestLongTransactions {

    public static final Log LOG = LogFactory.getLog(TestLongTransactions.class);
    HdfsConfiguration conf = null;
    boolean test_status = true; // means test has passed. in case of error threads will set it to false
    int wait_time = 10 * 1000;
    long start_time = 0;
    boolean test_started = false; // first thread will set it to true. 

    @Before
    public void initialize() throws Exception {
        conf = new HdfsConfiguration();
        StorageFactory.setConfiguration(conf);
        StorageFactory.getConnector().formatStorage();
    }

    @After
    public void close() {
    }

    @Test
    public void testDeleteOnExit() {
        try {


            start_time = System.currentTimeMillis();
            // just put on row in a table and make two transactins 
            // wait on it 
            insertData();

            int waitTime;

            Runnable w1 = new worker("T1");
            Runnable w2 = new worker("T2");

            Thread t1 = new Thread(w1);
            Thread t2 = new Thread(w2);



            t1.start();
            //Thread.sleep(1000);
            t2.start();

            t2.join();
            t1.join();

            if (!test_status) {
                fail("Test failed. Two transactions got the write lock at the same time");
            }

        } catch (Exception e) // all exceptions are bad
        {
            e.printStackTrace();
            fail("Test Failed");
        }
    }

    private void insertData() throws StorageException {
        System.out.println("Building the data...");
        List<INode> newFiles = new LinkedList<INode>();
        INodeFile root;
        root = new INodeFile(false, new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), (short) 3, 0l, 0l, 0l);
        root.setId(0);
        root.setName("/");
        root.setParentId(-1);

        INodeFile dir;
        dir = new INodeFile(false, new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), (short) 3, 0l, 0l, 0l);
        dir.setId(1);
        dir.setName("test_dir");
        dir.setParentId(0);

        INodeFile file;
        file = new INodeFile(false, new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), (short) 3, 0l, 0l, 0l);
        file.setId(2);
        file.setName("file1");
        file.setParentId(1);

        newFiles.add(root);
        newFiles.add(dir);
        newFiles.add(file);

        StorageFactory.getConnector().beginTransaction();
        InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
        da.prepare(new LinkedList<INode>(), newFiles, new LinkedList<INode>());
        StorageFactory.getConnector().commit();
    }

    private class worker implements Runnable {

        private String name;

        public worker(String name) {
            this.name = name;
        }

        @Override
        public void run() {

            beginTx();
            InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
            readRowWithRCLock(da, "/", -1);
            readRowWithRCLock(da, "test_dir", 0);
            readRowWithRCLock(da, "file1", 1);
            readRowWithRCLock(da, "/", -1);
            readRowWithWriteLock(da, "test_dir", 0);

            commitTx();

        }

        private void beginTx() {
            try {
                printMsg("Tx Start");
                StorageFactory.getConnector().beginTransaction();
            } catch (StorageException e) {
                test_status = false;
                System.err.println("Test Failed Begin Tx ");
                e.printStackTrace();
                fail("Test Failed");
            }
        }

        private void commitTx() {
            try {
                printMsg("Commiting");
                StorageFactory.getConnector().commit();
            } catch (StorageException e) {
                test_status = false;
                System.err.println("Test Failed Commit Tx ");
                e.printStackTrace();
                fail("Test Failed");
            }
        }

        private void readRowWithWriteLock(InodeDataAccess da, String name, long parent_id) {
            try {

                StorageFactory.getConnector().writeLock();
                INode file = (INodeFile) da.findInodeByNameAndParentId(name, parent_id);
                printMsg("ReadC Lock " + "pid: " + file.getParentId() + ", id:" + file.getId() + ", name:" + file.getName());
            } catch (StorageException e) {
                test_status = false;
                
                System.err.println("Test Failed ReadRowWithWriteLock. name "+name+" paretn_id "+parent_id);
                e.printStackTrace();
                fail("Test Failed");
            }
        }

        private void readRowWithRCLock(InodeDataAccess da, String name, long parent_id) {
            try {

                StorageFactory.getConnector().readCommitted();
                INode file = (INodeFile) da.findInodeByNameAndParentId(name, parent_id);
                printMsg("ReadC Lock " + "pid: " + file.getParentId() + ", id:" + file.getId() + ", name:" + file.getName());
            } catch (StorageException e) {
                test_status = false;
                System.err.println("Test Failed ReadRowWithRCLock. name "+name+" paretn_id "+parent_id+" exception " );
                e.printStackTrace();
                fail("Test Failed");
            }
        }

        private void printReplicas(List<IndexedReplica> replicas) {
            for (int i = 0; i < replicas.size(); i++) {
                IndexedReplica replica = replicas.get(i);
                printMsg(name + " " + replica.toString());
            }
        }

        private void printMsg(String msg) {
            System.out.println((System.currentTimeMillis() - start_time) + " " + " " + name + " " + msg);
        }
    }
}