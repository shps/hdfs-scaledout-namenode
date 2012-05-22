package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaMetrics {

  private AtomicLong selectUsingIndex;
  private AtomicLong delete;
  private AtomicLong insert;
  
  public ReplicaMetrics() {
    this.selectUsingIndex = new AtomicLong();
    this.delete = new AtomicLong();
    this.insert = new AtomicLong();
  }

  public void reset() {
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

  @Override
  public String toString() {
    long suIndex = getSelectUsingIndex();
    long insrt = getInsert();
    long dlt = getDelete();
    return String.format("\n==================== Replica Metrics ====================\n"
            + "Select Using Index: %d\n"
            + "Insert: %d\n"
            + "Delete: %d\n"
            +"Total: %d\n", suIndex, insrt, dlt,
            suIndex + insrt + dlt);
  }
}
