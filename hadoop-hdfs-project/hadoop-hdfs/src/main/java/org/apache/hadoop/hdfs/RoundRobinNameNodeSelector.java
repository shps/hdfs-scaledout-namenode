package org.apache.hadoop.hdfs;

import java.util.List;

/** An implementation of NameNodeSelector class for RoundRobin selection of read and write operations
 * So that appropriate namenodes can be selected for each operation
 */
public class RoundRobinNameNodeSelector extends NameNodeSelector {

  int currentIndex = 0;

public RoundRobinNameNodeSelector() {
  
}
public RoundRobinNameNodeSelector(List<DFSClient> namenodes) {
  this.namenodes = namenodes;
}

  /**Gets the appropriate reader namenode for a read operation
   * @return NameNode
   */
  @Override
  public DFSClient getNamenode() {
    DFSClient client = namenodes.get(currentIndex);

    currentIndex++;

    // Roll back to start when we reach the end of list
    if (currentIndex >= namenodes.size()) {
      currentIndex = 0;
    }

    return client;
  }
}