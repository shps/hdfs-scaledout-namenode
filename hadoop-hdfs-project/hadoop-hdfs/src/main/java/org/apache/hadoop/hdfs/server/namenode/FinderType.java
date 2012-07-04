package org.apache.hadoop.hdfs.server.namenode;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public interface FinderType<T> {

  public Class getType();
}
