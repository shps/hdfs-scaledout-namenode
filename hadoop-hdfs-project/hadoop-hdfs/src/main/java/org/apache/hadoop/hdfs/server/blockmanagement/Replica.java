package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class Replica {

  protected String storageId;
  protected long blockId;

  public Replica(String storageId, long blockId) {
    this.storageId = storageId;
    this.blockId = blockId;
  }

  /**
   * @return the storageId
   */
  public String getStorageId() {
    return storageId;
  }

  /**
   * @param storageId the storageId to set
   */
  public void setStorageId(String storageId) {
    this.storageId = storageId;
  }

  /**
   * @return the blockId
   */
  public long getBlockId() {
    return blockId;
  }

  /**
   * @param blockId the blockId to set
   */
  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Replica)) {
      return false;
    }

    Replica other = (Replica) obj;
    return this.blockId == other.getBlockId()
            && this.storageId.equals(other.getStorageId());
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 23 * hash + (this.storageId != null ? this.storageId.hashCode() : 0);
    hash = 23 * hash + (int) (this.blockId ^ (this.blockId >>> 32));
    return hash;
  }
}
