
package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class GenerationStampDataAccess extends EntityDataAccess{
  
  public static final String TABLE_NAME = "generation_stamp";
  public static final String ID = "id";
  public static final String COUNTER = "counter";
  
  public abstract Long findCounter() throws StorageException;
  public abstract void prepare(long counter) throws StorageException;
  
}
