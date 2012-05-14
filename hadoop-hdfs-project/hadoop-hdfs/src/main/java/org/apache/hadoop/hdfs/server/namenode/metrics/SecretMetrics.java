package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Hooman <hooman@sics.se>
 */
public class SecretMetrics {

  private AtomicLong selectUsingPKey;
  private AtomicLong selectUsingIndex;
  private AtomicLong selectAll;
  private AtomicLong delete;
  private AtomicLong update;
  private AtomicLong insert;

  public SecretMetrics() {
    this.selectUsingIndex = new AtomicLong();
    this.selectUsingPKey = new AtomicLong();
    this.delete = new AtomicLong();
    this.insert = new AtomicLong();
    this.update = new AtomicLong();
    this.selectAll = new AtomicLong();
  }

  public void reset() {
    this.selectUsingIndex.set(0);
    this.selectUsingPKey.set(0);
    this.delete.set(0);
    this.insert.set(0);
    this.update.set(0);
    this.selectAll.set(0);
  }

  public long getSelectAll() {
    return selectAll.get();
  }

  public long incrSelectAll() {
    return this.selectAll.incrementAndGet();
  }

  public long getUpdate() {
    return this.update.get();
  }

  public long incrUpdate() {
    return this.update.incrementAndGet();
  }

  /**
   * @return the selectUsingPKey
   */
  public long getSelectUsingPKey() {
    return selectUsingPKey.get();
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
   * @param selectUsingPKey the selectUsingPKey to inc
   */
  public long incrSelectUsingPKey() {
    return this.selectUsingPKey.incrementAndGet();
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
    long supKey = getSelectUsingPKey();
    long suIndex = getSelectUsingIndex();
    long suAll = getSelectAll();
    long updt = getUpdate();
    long insrt = getInsert();
    long dlt = getDelete();
    return String.format("\n==================== Secret Metrics ====================\n"
            + "Select Using Primary Key: %d\n"
            + "Select Using Index: %d\n"
            + "Select All: %d\n"
            + "Update: %d\n"
            + "Insert: %d\n"
            + "Delete: %d\n"
            + "Total: %d\n", supKey, suIndex, suAll, updt, insrt, dlt,
              supKey + suIndex + suAll + updt + insrt + dlt);
  }
}
