package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 * This class holds the information of one replica of a block in one datanode.
 * 
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public class Replica {
  long blockId;
  String storageId;
  int index;

  public Replica(long blockId, String storageId, int index) {
    this.blockId = blockId;
    this.storageId = storageId;
    this.index = index;
  }
  
  public long getBlockId() {
    return blockId;
  }
  
  public String getStorageId() {
    return storageId;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }
  
  @Override
  public boolean equals(Object obj) {
    Replica that = (Replica) obj;
    
    if (this == that)
      return true;
    else if (this.blockId == that.getBlockId() && this.getStorageId().equals(that.getStorageId()))
      return true;
    
    return false;
  }
  
  public String cacheKey() {
    StringBuilder builder = new StringBuilder(blockId + storageId);
    return builder.toString();
  }
}
