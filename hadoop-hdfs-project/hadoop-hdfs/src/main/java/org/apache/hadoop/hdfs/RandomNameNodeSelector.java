package org.apache.hadoop.hdfs;

import java.util.List;
import java.util.Random;

/** An implementation of NameNodeSelector class for Random Namenode selection of read and write operations
 * So that appropriate reader/ writer namenodes can be selected for each operation
 */
class RandomNameNodeSelector extends NameNodeSelector {

  /**Gets the appropriate reader namenode for a read operation
   * @return DFSClient
   */
  @Override
  public DFSClient getReaderNameNode() {
    Random randIndex = new Random(this.readerNameNodes.size());
    return readerNameNodes.get(randIndex.nextInt());
  }

  /**Gets the appropriate writer namenode for a read/write operation
   * @return DFSClient
   */
  @Override
  public DFSClient getWriterNameNode() {
    Random randIndex = new Random(this.readerNameNodes.size());
    return writerNameNodes.get(randIndex.nextInt());
  }
}
