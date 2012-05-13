package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockTotalMetrics{

  private AtomicLong select;
  private AtomicLong update;

  public BlockTotalMetrics() {
    select = new AtomicLong();
    update = new AtomicLong();
  }

  public void reset() {
    select.set(0);
    update.set(0);
  }

  public long getSelect() {
    return select.get();
  }

  public long incrSelect() {
    return select.incrementAndGet();
  }

  public long getUpdate() {
    return update.get();
  }

  public long incrUpdate() {
    return update.incrementAndGet();
  }

  @Override
  public String toString() {
    return String.format("==================== BlockTotal Metrics ====================\n"
            + "Select: %d\n"
            + "Update: %d\n",
            getSelect(), getUpdate());
  }
}
