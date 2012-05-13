package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathMetrics {

  private AtomicLong selectAll;
  private AtomicLong selectUsingIndex;
  private AtomicLong delete;
  private AtomicLong insert;

  public LeasePathMetrics() {
    this.selectUsingIndex = new AtomicLong();
    this.selectAll = new AtomicLong();
    this.delete = new AtomicLong();
    this.insert = new AtomicLong();
  }

  public void reset() {
    this.selectAll.set(0);
    this.selectUsingIndex.set(0);
    this.delete.set(0);
    this.insert.set(0);
  }

  /**
   * @return the selectUsingIndex
   */
  public long getSelectUsingIndex() {
    return selectUsingIndex.get();
  }

  /**
   * @return the deleteUsingPkey
   */
  public long getDelete() {
    return delete.get();
  }

  /**
   * @return the insertUsingPKey
   */
  public long getInsert() {
    return insert.get();
  }

  /**
   * @param selectUsingIndex the selectUsingIndex to inc
   */
  public long incrSelectUsingIndex() {
    return this.selectUsingIndex.incrementAndGet();
  }

  /**
   * @param deleteUsingPkey the deleteUsingPkey to inc
   */
  public long incrDelete() {
    return this.delete.incrementAndGet();
  }

  /**
   * @param insertUsingPKey the insertUsingPKey to inc
   */
  public long incrInsert() {
    return this.insert.incrementAndGet();
  }

  /**
   * @return the selectAll
   */
  public long getSelectAll() {
    return selectAll.get();
  }

  /**
   * @param selectAll the selectAll to set
   */
  public long incrSelectAll() {
    return this.selectAll.incrementAndGet();
  }

  @Override
  public String toString() {
    return String.format("==================== LeasePath Metrics ====================\n"
            + "Select Using Index: %d\n"
            + "Insert: %d\n"
            + "Delete: %d\n", getSelectUsingIndex(),
            getInsert(), getDelete());
  }
}
