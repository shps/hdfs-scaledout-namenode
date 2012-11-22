package org.apache.hadoop.hdfs.server.namenode;

import java.io.Serializable;

/**
 *
 * @author jude
 */
  public class INodeCachedEntry implements Serializable{
    private long id;
    private long parentid;
    String name;
    
    public INodeCachedEntry(long id, long parentid, String name) {
      this.id=id;
      this.parentid = parentid;
      this.name = name;
    }

    public long getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public long getParentid() {
      return parentid;
    }
    
  }
