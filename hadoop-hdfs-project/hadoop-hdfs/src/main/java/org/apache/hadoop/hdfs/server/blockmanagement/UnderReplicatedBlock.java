package org.apache.hadoop.hdfs.server.blockmanagement;

public class UnderReplicatedBlock {

  int level;
  long blockId;

  public UnderReplicatedBlock(int level, long blockId) {
    this.level = level;
    this.blockId = blockId;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getLevel() {
    return level;
  }

  @Override
  public String toString() {
    return "UnderReplicatedBlock{" + "level=" + level + ", blockId=" + blockId + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final UnderReplicatedBlock other = (UnderReplicatedBlock) obj;
    if (this.level != other.level) {
      return false;
    }
    if (this.blockId != other.blockId) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + this.level;
    hash = 97 * hash + (int) (this.blockId ^ (this.blockId >>> 32));
    return hash;
  }
}
