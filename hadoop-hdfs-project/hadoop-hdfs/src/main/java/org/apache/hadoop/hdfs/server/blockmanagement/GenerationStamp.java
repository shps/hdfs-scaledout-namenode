package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class GenerationStamp {

  public static enum Finder implements org.apache.hadoop.hdfs.server.namenode.FinderType<GenerationStamp> {
    
    // Value of the generation stamp.
    Counter;

    @Override
    public Class getType() {
      return GenerationStamp.class;
    }
  }
  private long counter;

  public GenerationStamp(long counter) {
    this.counter = counter;
  }

  public long getCounter() {
    return counter;
  }
  
  public void setCounter(long counter)
  {
    this.counter = counter;
  }
}
