package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public interface LeasePathDataAccess {

  public static final String TABLE_NAME = "lease_paths";
  public static final String HOLDER_ID = "holder_id";
  public static final String PATH = "path";

  public Collection<LeasePath> findByHolderId(int holderId) throws StorageException;

  public Collection<LeasePath> findByPrefix(String prefix) throws StorageException;

  public Collection<LeasePath> findAll() throws StorageException;

  public LeasePath findByPKey(String path) throws StorageException;

  public void prepare(Collection<LeasePath> removed, Collection<LeasePath> newed, Collection<LeasePath> modified) throws StorageException;
}
