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
 * @author Hooman <hooman@sics.se>
 */
public class TestLongTransactions {

    public static final Log LOG = LogFactory.getLog(TestLongTransactions.class);
    HdfsConfiguration conf = null;
    boolean test_status = true; // means test has pass. in case of error threads will set it to false
    int wait_time = 10*1000;
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



            // just put on row in a table and make two transactins 
            // wait on it 
            insertData();

            int waitTime; 
            
            Runnable w1 = new worker("T1");
            Runnable w2 = new worker("T2");
            
            Thread t1 = new Thread(w1);
            Thread t2 = new Thread(w2);
            
            
            start_time = System.currentTimeMillis();
            t1.start();
            t2.start();
            
            t1.join();
            t2.join();
            
            if(!test_status)
            {
                fail("Test failed. Two transactions got the write lock at the same time");
            }

        } catch (Exception e) // all exceptions are bad
        {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void insertData() throws StorageException {
        System.out.println("Building the data...");
        IndexedReplica r = new IndexedReplica(1, "1", 1);
        List<IndexedReplica> newReplicas = new LinkedList<IndexedReplica>();
        newReplicas.add(r);

        StorageFactory.getConnector().beginTransaction();
        ReplicaDataAccess da = (ReplicaDataAccess) StorageFactory.getDataAccess(ReplicaDataAccess.class);
        da.prepare(new LinkedList<IndexedReplica>(), newReplicas, new LinkedList<IndexedReplica>());
        StorageFactory.getConnector().commit();
    }

    private class worker implements Runnable 
    {
        private String name;
        public worker(String name)
        {
            this.name = name;
        }
        
        @Override
        public void run() {

            try {
                // get a Write lock on the inserted row and then wait for 10 sec
                // change the value
                StorageFactory.getConnector().writeLock();
                StorageFactory.getConnector().beginTransaction();
                
                if(!test_started)
                {
                    test_started = true;
                }
                else
                {
                    // ideally send thread should get here after 'wait_time'
                    if(System.currentTimeMillis() - wait_time < wait_time)
                    {
                        test_status = false;
                        StorageFactory.getConnector().rollback();
                        return;
                    }
                }
                
                
                ReplicaDataAccess da = (ReplicaDataAccess) StorageFactory.getDataAccess(ReplicaDataAccess.class);
                List<IndexedReplica> replicas = da.findReplicasById(1);
                for(int i = 0; i < replicas.size(); i++)
                {
                    IndexedReplica replica = replicas.get(i);
                    System.out.println(name+" "+replica.toString());
                    replica.setBlockId(2);
                    replica.setStorageId("2");
                    replica.setIndex(2);
                }
                
                Thread.sleep(wait_time); 
                da.prepare(new LinkedList<IndexedReplica>(), replicas, new LinkedList<IndexedReplica>());
                replicas = da.findReplicasById(1);
                for(int i = 0; i < replicas.size(); i++)
                {
                    IndexedReplica replica = replicas.get(i);
                    System.out.println(name+" "+replica.toString());
                }
                StorageFactory.getConnector().commit();
            } catch (Exception e) {
                fail(e.getMessage());
            }

        }
        

    }
}