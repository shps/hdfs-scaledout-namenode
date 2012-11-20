package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.net.NetUtils;

/**
 *
 * @author jude
 */
public class ActiveNamenodeList implements Writable {

  SortedMap<Long, InetSocketAddress> namenodes = new TreeMap<Long, InetSocketAddress>();
  
  public ActiveNamenodeList() {
    
  }
  public ActiveNamenodeList(SortedMap<Long, InetSocketAddress> namenodes) {
    this.namenodes = namenodes;
  }
  
  public SortedMap<Long, InetSocketAddress> getNamenodes() {
    return namenodes;
  }
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    for(long nn : namenodes.keySet()) {
      str.append("[").append(nn).append("]").append(namenodes.get(nn).getAddress().getHostAddress()).append(":").append(namenodes.get(nn).getPort());
    }
    return str.toString();
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (ActiveNamenodeList.class,
       new WritableFactory() {
         public Writable newInstance() { return new ActiveNamenodeList(); }
       });
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    namenodes = new TreeMap<Long, InetSocketAddress>();
    
    int size = in.readInt();
    for(int i=0; i<size; i++) {
      long namenodeId = in.readLong();
      String hostname = Text.readString(in);
      InetSocketAddress addr = NetUtils.createSocketAddr(hostname);
      namenodes.put(namenodeId, addr);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // writing total namenodes first
    out.writeInt(namenodes.size());
    for(long nn : namenodes.keySet()) {
      out.writeLong(nn);
      String hostname = namenodes.get(nn).getAddress().getHostAddress()+":"+namenodes.get(nn).getPort();
      Text.writeString(out,hostname);
    }
  }
  
}
