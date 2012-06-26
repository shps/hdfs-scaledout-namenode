
package org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby;

import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.INodeStorage;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class INodeDerby extends INodeStorage{

  DerbyConnector connector = DerbyConnector.INSTANCE;
  @Override
  protected INode findInodeById(long inodeId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected List<INode> findInodesByParentIdSortedByName(long parentId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected INode findInodeByNameAndParentId(String name, long parentId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected List<INode> findInodesByIds(List<Long> ids) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int countAll() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commit() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
