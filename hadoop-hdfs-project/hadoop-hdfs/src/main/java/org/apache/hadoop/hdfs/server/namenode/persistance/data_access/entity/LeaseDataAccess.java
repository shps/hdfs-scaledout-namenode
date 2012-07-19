package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public interface LeaseDataAccess {

  public static final String TABLE_NAME = "leases";
  public static final String HOLDER = "holder";
  public static final String LAST_UPDATE = "last_update";
  public static final String HOLDER_ID = "holder_id";

  public int countAll() throws StorageException;

  public Collection<Lease> findByTimeLimit(long timeLimit) throws StorageException;

  public Collection<Lease> findAll() throws StorageException;

  public Lease findByPKey(String holder) throws StorageException;

  public Lease findByHolderId(int holderId) throws StorageException;

  public void prepare(Collection<Lease> removed, Collection<Lease> newed, Collection<Lease> modified) throws StorageException;
}
