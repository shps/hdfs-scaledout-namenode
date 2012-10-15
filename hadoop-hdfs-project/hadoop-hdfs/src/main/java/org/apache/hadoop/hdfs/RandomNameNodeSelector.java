package org.apache.hadoop.hdfs;

import java.util.Random;

/** An implementation of NameNodeSelector class for Random Namenode selection of read and write operations
 * So that appropriate namenodes can be selected for each operation
 */
class RandomNameNodeSelector extends NameNodeSelector {

  /**Gets the appropriate reader namenode for a read/write operation
   * @return DFSClient
   */
  @Override
  public DFSClient getNamenode() {
    Random randIndex = new Random(this.namenodes.size());
    return namenodes.get(randIndex.nextInt());
  }
}
