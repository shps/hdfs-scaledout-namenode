package org.apache.hadoop.hdfs.server.namenode.persistance;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public interface Finder<T> {

  public Class getType();
}
