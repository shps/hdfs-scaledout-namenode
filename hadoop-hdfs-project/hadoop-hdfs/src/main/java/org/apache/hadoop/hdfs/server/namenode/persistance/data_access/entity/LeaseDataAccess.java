package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public abstract class LeaseDataAccess extends EntityDataAccess{

  public static final String TABLE_NAME = "leases";
  public static final String HOLDER = "holder";
  public static final String LAST_UPDATE = "last_update";
  public static final String HOLDER_ID = "holder_id";

  public abstract int countAll() throws StorageException;

  public abstract Collection<Lease> findByTimeLimit(long timeLimit) throws StorageException;

  public abstract Collection<Lease> findAll() throws StorageException;

  public abstract Lease findByPKey(String holder) throws StorageException;

  public abstract Lease findByHolderId(int holderId) throws StorageException;

  public abstract void prepare(Collection<Lease> removed, Collection<Lease> newLeases, Collection<Lease> modified) throws StorageException;
}
