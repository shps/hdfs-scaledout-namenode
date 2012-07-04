package org.apache.hadoop.hdfs.server.namenode;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePath implements Comparable<LeasePath> {

  public static enum Finder implements org.apache.hadoop.hdfs.server.namenode.FinderType<LeasePath> {

    ByHolderId, ByPKey, ByPrefix, All;

    @Override
    public Class getType() {
      return LeasePath.class;
    }
  }
  private int holderId;
  private String path;

  public LeasePath(String path, int holderId) {
    this.holderId = holderId;
    this.path = path;
  }

  /**
   * @return the holderId
   */
  public int getHolderId() {
    return holderId;
  }

  /**
   * @param holderId the holderId to set
   */
  public void setHolderId(int holderId) {
    this.holderId = holderId;
  }

  /**
   * @return the path
   */
  public String getPath() {
    return path;
  }

  /**
   * @param path the path to set
   */
  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public int compareTo(LeasePath t) {
    return this.path.compareTo(t.getPath());
  }

  @Override
  public boolean equals(Object obj) {
    LeasePath other = (LeasePath) obj;
    return (this.path.equals(other.getPath()) && this.holderId == other.getHolderId());
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + (this.path != null ? this.path.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    return this.path;
  }
}
