package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class BlockTokenKeyDataAccess extends EntityDataAccess {

  /**
   * delegation key table info
   */
  public static final String TABLE_NAME = "block_token_keys";
  public static final String KEY_ID = "key_id";
  public static final String EXPIRY_DATE = "expiry_date";
  public static final String KEY_BYTES = "key_bytes";
  public static final String KEY_TYPE = "key_type";
  
  public abstract BlockKey findByKeyId(int keyId) throws StorageException;
  public abstract BlockKey findByKeyType(short keyType) throws StorageException;
  public abstract List<BlockKey> findAll() throws StorageException;
  public abstract void prepare(Collection<BlockKey> removed, Collection<BlockKey> newed, Collection<BlockKey> modified) throws StorageException;
  public abstract void removeAll() throws StorageException;
}
