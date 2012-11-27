package org.apache.hadoop.hdfs.server.namenode.persistance;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import se.sics.clusterj.CountersTable;

/**
 *
 * @author jude
 */
public class CountersHelper {
  static final int RETRY_COUNT = 3;
  public static final int LEADER_COUNTER_ID =1;
  public static final int GS_COUNTER_ID = 2;
  
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static void resetAllCounters() {
    Session session = DBConnector.obtainSession();
    
    assert session.currentTransaction().isActive();
    
    // Reset leader counters
    CountersTable ldCounter = session.newInstance(CountersTable.class);
    ldCounter.setId(LEADER_COUNTER_ID);
    ldCounter.setName("Leader Election");
    ldCounter.setValue(0);
    session.savePersistent(ldCounter);
            
    // Reset GS counters
    CountersTable gsCounter = session.newInstance(CountersTable.class);
    gsCounter.setId(GS_COUNTER_ID);
    gsCounter.setName("Generation Stamp");
    gsCounter.setValue(0);
    session.savePersistent(gsCounter);
    
    session.deletePersistentAll(CountersTable.class);
  }
  
  // Increment manual
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static long updateCounter(int id, long value) {
    Session session = DBConnector.obtainSession();
    assert session.currentTransaction().isActive();
    
    //session.setLockMode(LockMode.EXCLUSIVE);
    CountersTable record = getCounterInternal(id);
    record.setValue(value);
    
    // save updated value
    updateCounterInternal(session, record);
    //session.setLockMode(LockMode.SHARED);
    return value;
  }

  // Increment automatically
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static long updateCounter(int id) {
    Session session = DBConnector.obtainSession();
    assert session.currentTransaction().isActive();
  
    //session.setLockMode(LockMode.EXCLUSIVE);
    CountersTable record = getCounterInternal(id);
    long updatedValue = record.getValue()+1;
    
    record.setValue(updatedValue);
    
    // save updated value
    updateCounterInternal(session, record);
    //session.flush();
    //session.setLockMode(LockMode.SHARED);
    return updatedValue;
  }

  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  public static long getCounterValue(int id) {
    return getCounterInternal(id).getValue();
  }
  
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private static CountersTable getCounterInternal(int id) {
    Session session = DBConnector.obtainSession();
    CountersTable record = session.find(CountersTable.class, id);
    if(record == null) {
      throw new ClusterJException("Reset the counters table for ID: "+ id);
    }
    return record;
  }
  
  // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  private static void updateCounterInternal(Session session, CountersTable record) {
    session.savePersistent(record);
  }
}
