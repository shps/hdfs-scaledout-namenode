
package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public enum INodeFinder implements Finder<INode> {
  ByPKey, ByParentId, ByNameAndParentId, ByIds;

  @Override
  public Class getType() {
    return INode.class;
  }
  
}
