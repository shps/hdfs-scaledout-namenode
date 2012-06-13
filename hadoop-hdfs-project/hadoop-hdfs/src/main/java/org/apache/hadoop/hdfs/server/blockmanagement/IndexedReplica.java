package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 * This class holds the information of one replica of a block in one datanode.
 * 
 * @author kamal hakimzadeh<kamal@sics.se>
 */
public class IndexedReplica extends Replica {

  int index;

  public IndexedReplica(long blockId, String storageId, int index) {
    super(storageId, blockId);
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }
  
  @Override
  public boolean equals(Object obj) {
    IndexedReplica that = (IndexedReplica) obj;
    
    if (this == that)
      return true;
    else if (blockId == that.getBlockId() && this.getStorageId().equals(that.getStorageId()))
      return true;
    
    return false;
  }
  
  public String cacheKey() {
    StringBuilder builder = new StringBuilder(blockId + storageId);
    return builder.toString();
  }
}
