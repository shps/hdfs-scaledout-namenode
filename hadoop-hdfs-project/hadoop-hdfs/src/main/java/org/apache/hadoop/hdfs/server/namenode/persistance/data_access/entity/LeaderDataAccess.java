package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.Leader;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Salman <salman@sics.se>
 */
public abstract class LeaderDataAccess extends EntityDataAccess {

  public static final String TABLE_NAME = "leader";
  public static final String ID = "id";
  public static final String COUNTER = "counter";
  public static final String TIMESTAMP = "timestamp";
  public static final String HOSTNAME = "hostname";
  public static final String AVG_REQUEST_PROCESSING_LATENCY = "avg_request_processing_latency";
  public static final String PARTITION_VAL = "partition_val";


  public abstract int countAll() throws StorageException;
  
//  public abstract Leader findById(long id) throws StorageException;
  
  public abstract Leader findByPkey(long id, int partitionKey) throws StorageException;

  public abstract Collection<Leader> findAllByCounterGT(long counter) throws StorageException;
  
  public abstract Collection<Leader> findAllByIDLT(long id) throws StorageException;
  
  public abstract Collection<Leader> findAll() throws StorageException;
  
  public abstract int countAllPredecessors(long id) throws StorageException;
  
  public abstract int countAllSuccessors(long id) throws StorageException;

  public abstract void prepare(Collection<Leader> removed, Collection<Leader> newed, Collection<Leader> modified) throws StorageException;
  
}
