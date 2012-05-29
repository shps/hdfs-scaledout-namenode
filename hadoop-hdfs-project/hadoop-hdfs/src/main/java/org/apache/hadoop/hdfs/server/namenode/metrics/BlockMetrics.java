
package org.apache.hadoop.hdfs.server.namenode.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Hooman <hooman@sics.se>
 */
public class BlockMetrics {
    

    private AtomicLong selectUsingPKey;
    private AtomicLong selectUsingIndex;
    private AtomicLong update;
    private AtomicLong delete;
    private AtomicLong insert;
    
    public BlockMetrics()
    {
        this.selectUsingIndex = new AtomicLong();
        this.selectUsingPKey = new AtomicLong();
        this.update = new AtomicLong();
        this.delete = new AtomicLong();
        this.insert = new AtomicLong();
    }
    
    public void reset()
    {
        this.selectUsingIndex.set(0);
        this.selectUsingPKey.set(0);
        this.update.set(0);
        this.delete.set(0);
        this.insert.set(0);
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
     * @return the updateUsingPKey
     */
    public long getUpdate() {
        return update.get();
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
     * @param updateUsingPKey the updateUsingPKey to inc
     */
    public long incrUpdate() {
        return this.update.incrementAndGet();
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
    public String toString()
    {
      long supKey = getSelectUsingPKey();
      long suIndex = getSelectUsingIndex();
      long updt = getUpdate();
      long insrt = getInsert();
      long dlt = getDelete();
      return String.format("\n==================== Block Metrics ====================\n"
              + "Select Using Primary Key: %d\n"
              + "Select Using Index: %d\n"
              + "Update: %d\n"
              + "Insert: %d\n"
              + "Delete: %d\n"
              + "Total: %d\n", supKey, suIndex, updt, insrt, dlt,
              supKey + suIndex + updt + insrt + dlt);
    }

}
