package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import java.util.LinkedList;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.log4j.NDC;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class TransactionalRequestHandler extends RequestHandler {
  
    public TransactionalRequestHandler(OperationType opType) {
        super(opType);
    }

    @Override
    protected Object run(boolean writeLock, boolean readLock, Namesystem namesystem) throws IOException {
        boolean systemLevelLock = FSNamesystem.systemLevelLock();
        boolean rowLevelLock = FSNamesystem.rowLevelLock();
        if (systemLevelLock) {
            if (writeLock) {
                namesystem.writeLock();
            }
            if (readLock) {
                namesystem.readLock();
            }
        }
        boolean retry = true;
        boolean rollback = false;
        int tryCount = 0;
        IOException exception = null;
        long txStartTime = 0;


        try {
            while (retry && tryCount < RETRY_COUNT) {
                retry = true;
                rollback = false;
                tryCount++;
                exception = null;

                long oldTime = 0;
                try {
                    // Defines a context for every operation to track them in the logs easily.
                    if (namesystem != null) {
                        NDC.push("NN (" + namesystem.getNamenodeId() + ") " + opType.name());
                    } else {
                        NDC.push(opType.name());
                    }
                    setUp();
                    txStartTime = System.currentTimeMillis();
                    oldTime = 0;
                    EntityManager.begin();
                    log.debug("tx started");
                    oldTime = System.currentTimeMillis();
                    if (rowLevelLock) {
                        acquireLock();
                        log.debug("all locks acquired  in " + (System.currentTimeMillis() - oldTime) + " msec");
                        oldTime = System.currentTimeMillis();
                        EntityManager.preventStorageCall();
                    }
                    log.debug("starting in memory processing");
                    Object obj = performTask();
                    log.debug("in memory processig finished  in " + (System.currentTimeMillis() - oldTime) + " msec");
                    oldTime = System.currentTimeMillis();
                    return obj;
                } catch (TransactionContextException ex) {
                    log.error("Could not perfortm task", ex);
                    rollback = true;
                    retry = false;
                } catch (PersistanceException ex) {
                    log.error("Tx FAILED. total tx time "+
                            (System.currentTimeMillis() - txStartTime)+
                            " msec. Retry("+
                            retry+") TotalRetryCount("+
                            RETRY_COUNT+") RemainingRetries("+
                            (RETRY_COUNT-tryCount)+")", ex);
                    rollback = true;
                    retry = true;
                } catch (IOException ex) {
                    exception = ex;
                } finally {
                    try {
                        if (!rollback) {
                            EntityManager.commit();
                            log.debug("tx committed. commit took " + (System.currentTimeMillis() - oldTime) + " msec");
                            log.debug("Total time for tx is " + (System.currentTimeMillis() - txStartTime) + " msec");
                        }
                    } catch (StorageException ex) {
                        log.error("Could not commit transaction", ex);
                        rollback = true;
                        retry = true;
                    } finally {
                        try {
                            if (rollback) {
                                try {
                                    EntityManager.rollback();
                                } catch (StorageException ex) {
                                    log.error("Could not rollback transaction", ex);
                                }
                            }
                            if (tryCount == RETRY_COUNT && exception != null) {
                                throw exception;
                            }
                        } finally {
                            NDC.pop();
                        }
                    }
                }
            }
        } finally {
            if (systemLevelLock) {
                if (writeLock) {
                    namesystem.writeUnlock();
                }
                if (readLock) {
                    namesystem.readUnlock();
                }
            }
        }
        return null;
    }

    public abstract void acquireLock() throws PersistanceException, IOException;

    @Override
    public TransactionalRequestHandler setParams(Object... params) {
        this.params = params;
        return this;
    }
    
    public void setUp() throws PersistanceException, IOException
    {
      // Do nothing.
      // This can be overriden.
    }
}
