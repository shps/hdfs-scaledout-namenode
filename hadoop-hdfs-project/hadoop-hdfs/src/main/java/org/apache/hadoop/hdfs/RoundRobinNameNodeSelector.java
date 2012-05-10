package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.List;

/** An implementation of NameNodeSelector class for RoundRobin selection of read and write operations
 * So that appropriate reader/ writer namenodes can be selected for each operation
 */
public class RoundRobinNameNodeSelector extends NameNodeSelector {

  int currentReadIndex = 0;
  int currentWriteIndex = 0;

public RoundRobinNameNodeSelector() {
  
}
public RoundRobinNameNodeSelector(List<DFSClient> readerNamenodes, List<DFSClient> writerNamenodes) {
  this.readerNameNodes = readerNamenodes;
  this.writerNameNodes = writerNamenodes;
}

  /**Gets the appropriate reader namenode for a read operation
   * @return NameNode
   */
  @Override
  public DFSClient getReaderNameNode() {
    DFSClient client = readerNameNodes.get(currentReadIndex);

    currentReadIndex++;

    // Roll back to start when we reach the end of list
    if (currentReadIndex >= readerNameNodes.size()) {
      currentReadIndex = 0;
    }

    return client;
  }

  /**Gets the appropriate writer namenode for a read/write operation
   * @return NameNode
   */
  @Override
  public DFSClient getWriterNameNode() {
    DFSClient client = writerNameNodes.get(currentWriteIndex);

    currentWriteIndex++;

    // Roll back to start when we reach the end of list
    if (currentWriteIndex >= writerNameNodes.size()) {
      currentWriteIndex = 0;
    }
    return client;
  }
  
}