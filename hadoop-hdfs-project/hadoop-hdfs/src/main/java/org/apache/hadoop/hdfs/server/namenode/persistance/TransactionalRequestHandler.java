package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class TransactionalRequestHandler {

  private static Log log = LogFactory.getLog(TransactionalRequestHandler.class);
  private Object[] params = new Object[8];
  public static final int RETRY_COUNT = 1;
  private boolean retry = true;
  private boolean rollback = false;
  private int tryCount = 0;
  private IOException exception = null;

  public TransactionalRequestHandler() {
  }

  public Object handle() throws IOException {
    return run(false, false, null);
  }

  public Object handleWithWriteLock(Namesystem namesystem) throws IOException {
    return run(true, false, namesystem);
  }

  public Object handleWithReadLock(Namesystem namesystem) throws IOException {
    return run(false, true, namesystem);
  }

  private Object run(boolean writeLock, boolean readLock, Namesystem namesystem) throws IOException {
    retry = true;
    rollback = false;
    tryCount = 0;
    exception = null;

    while (retry && tryCount < RETRY_COUNT) {
      retry = true;
      rollback = false;
      tryCount++;
      exception = null;
      try {
        if (writeLock) {
          namesystem.writeLock();
        }
        if (readLock) {
          namesystem.readLock();
        }
        EntityManager.begin();
        return performTask();
      } catch (TransactionContextException ex) {
        log.error("Could not perfortm task", ex);
        rollback = true;
        retry = false;
      } catch (PersistanceException ex) {
        log.error("Could not perfortm task", ex);
        rollback = true;
        retry = true;
      } catch (IOException ex) {
        this.exception = ex;
      } finally {
        try {
          if (!rollback) {
            EntityManager.commit();
          }
        } catch (StorageException ex) {
          log.error("Could not commit transaction", ex);
          rollback = true;
          retry = true;
        } finally {
          if (rollback) {
            try {
              EntityManager.rollback();
            } catch (StorageException ex) {
              log.error("Could not rollback transaction", ex);
            }
          }
          if (!retry && exception != null) {
            throw exception;
          }
          if (writeLock) {
            namesystem.writeUnlock();
          }
          if (readLock) {
            namesystem.readUnlock();
          }
        }

      }
    }
    return null;
  }

  public abstract Object performTask() throws PersistanceException, IOException;

  public Object getParam1() {
    return params[0];
  }

  public TransactionalRequestHandler setParam1(Object param1) {
    this.params[0] = param1;
    return this;
  }

  public Object getParam2() {
    return params[1];
  }

  public TransactionalRequestHandler setParam2(Object param2) {
    this.params[1] = param2;
    return this;
  }

  public Object getParam3() {
    return params[2];
  }

  public TransactionalRequestHandler setParam3(Object param3) {
    this.params[2] = param3;
    return this;
  }

  public Object getParam4() {
    return params[3];
  }

  public TransactionalRequestHandler setParam4(Object param4) {
    this.params[3] = param4;
    return this;
  }

  public Object getParam5() {
    return params[4];
  }

  public TransactionalRequestHandler setParam5(Object param5) {
    this.params[4] = param5;
    return this;
  }

  public Object getParam6() {
    return params[5];
  }

  public TransactionalRequestHandler setParam6(Object param6) {
    this.params[5] = param6;
    return this;
  }

  public TransactionalRequestHandler setParam7(Object param7) {
    this.params[6] = param7;
    return this;
  }

  public Object getParam7() {
    return params[6];
  }

  public TransactionalRequestHandler setParam8(Object param8) {
    this.params[7] = param8;
    return this;
  }

  public Object getParam8() {
    return params[7];
  }
}
