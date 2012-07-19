package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class TransactionalRequestHandler {

  Object param1;
  Object param2;
  Object param3;
  Object param4;
  Object param5;
  Object param6;
  Object param7;  
  Object param8;
  
  public Object getParam1() {
    return param1;
  }

  public TransactionalRequestHandler setParam1(Object param1) {
    this.param1 = param1;
    return this;
  }
  
  public Object getParam2() {
    return param2;
  }

  public TransactionalRequestHandler setParam2(Object param2) {
    this.param2 = param2;
    return this;
  }
  
  public Object getParam3() {
    return param3;
  }

  public TransactionalRequestHandler setParam3(Object param3) {
    this.param3 = param3;
    return this;
  }
  
  public Object getParam4() {
    return param4;
  }

  public TransactionalRequestHandler setParam4(Object param4) {
    this.param4 = param4;
    return this;
  }

  public Object getParam5() {
    return param5;
  }

  public TransactionalRequestHandler setParam5(Object param5) {
    this.param5 = param5;
    return this;
  }

  public Object getParam6() {
    return param6;
  }

  public TransactionalRequestHandler setParam7(Object param7) {
    this.param7 = param7;
    return this;
  }

  public Object getParam7() {
    return param7;
  }

  public TransactionalRequestHandler setParam8(Object param8) {
    this.param8 = param8;
    return this;
  }

  public Object getParam8() {
    return param8;
  }

  public TransactionalRequestHandler setParam6(Object param6) {
    this.param6 = param6;
    return this;
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
    EntityManager.aboutToStart();
    while (EntityManager.shouldRetry()) {
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
        EntityManager.setRollbackOnly();
      } catch (PersistanceException ex) {
        EntityManager.setRollbackAndRetry();
      } catch (IOException ex) {
        EntityManager.toBeThrown(ex);
      } finally {
        try {
          if (!EntityManager.shouldRollback()) {
            EntityManager.commit();
          }
        } catch (StorageException ex) {
          EntityManager.setRollbackAndRetry();
        } finally {
          if (EntityManager.shouldRollback()) {
            EntityManager.rollback();
          }
          if (!EntityManager.shouldRetry() && EntityManager.shouldThrow()) {
            throw EntityManager.getException();
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
}
