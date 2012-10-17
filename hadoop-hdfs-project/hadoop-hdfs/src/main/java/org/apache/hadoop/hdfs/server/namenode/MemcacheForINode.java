
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/**
 * MemcacheForINode tutorial
 * [http://sacharya.com/using-memcached-with-java/]
 * [http://www.clusterdb.com/mysql-cluster/scalabale-persistent-ha-nosql-memcache-storage-using-mysql-cluster/]
 * 
 * Run memcache:
 * memcached -uroot -m 1024 -d 127.0.0.1 -p 11211
 *
 */
public class MemcacheForINode {

  static final int TIMEOUT = 0;
  static final int POOL_SIZE=20;
  
  static final Log LOG = LogFactory.getLog(MemcacheForINode.class);
  protected boolean isConnected = false;
  private static MemcacheForINode instance;
  
  ExecutorService executor;
  protected MemcachedClient [] cache_readers;
  protected MemcachedClient [] cache_writers;
  protected int index_reader = -1;
  protected int index_writer = -1;
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  private MemcacheForINode() {
    
  }

  public synchronized static MemcacheForINode getInstance() {
    if(instance == null) {
      instance = new MemcacheForINode();
    }
    return instance;
  }
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void setConfiguration(Configuration conf) throws IOException {
    if(!isConnected) {
      String [] servers = conf.get(DFSConfigKeys.DFS_MEMCACHE_SERVERS_KEY, DFSConfigKeys.DFS_MEMCACHE_SERVERS_DEFAULT).split(",");
      String serverAddresses = "";
      for(String s : servers) {
        serverAddresses+= s + " ";
      }

      // worker pool
      executor =  Executors.newFixedThreadPool(POOL_SIZE*2);
      
      // initializing the cache client pool
      cache_readers = new MemcachedClient[POOL_SIZE];
      cache_writers = new MemcachedClient[POOL_SIZE];

      for(int i=0; i<POOL_SIZE; i++) {
        cache_readers[i] = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(serverAddresses.trim()));
        cache_writers[i] = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(serverAddresses.trim()));
      }
      isConnected = true;
      LOG.info("Memcache client connected to server ["+serverAddresses+"]");
    }
  }

  /**
   * gets the next reader cache
   */
  private int getNextReaderCache() {
    index_reader++;
    index_reader = index_reader % POOL_SIZE;
    return index_reader;
  }
  /**
   * gets the next writer cache
   */
  private int getNextWriterCache() {
    index_writer++;
    index_writer = index_writer % POOL_SIZE;
    return index_writer;
  }
  /**
   * I need to return the last inode from the data structure in O(1), 
   * but no data structure to help me get it [org.apache.commons.collections15.map.ListOrderedMap<K,V>]
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void putINode(String path, ListOrderedMap<Long, INodeCachedEntry> inodesOnPath) {
    //LOG.debug("storing inodes in path ["+path.toString()+"] to cache");
    executor.execute(new CacheWriter(cache_writers[getNextWriterCache()], path, inodesOnPath, CacheWriter.SET_OP));
  }

  /**
   * gets the inode entries that lie on 'path' and verifies the entries are correct via the db
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public INode[] getINodesOnPath(String path) {
    ListOrderedMap<Long, INodeCachedEntry> cachedEntries=  (ListOrderedMap<Long, INodeCachedEntry>) cache_readers[getNextReaderCache()].get(path);
    if(cachedEntries == null || cachedEntries.isEmpty()) {
       return null;
    }
    INode [] inodesDB = new INode [cachedEntries.size()];
    // if verified from DB to be correct, return the inode and save it in 'inodesDB'
    if(verify(cachedEntries, inodesDB)) {
      //LOG.info("cache hit for path: ["+path+"]");
      return inodesDB;
    }
    // not correct in db. remove from cache
    LOG.debug("Path ["+path+"] not consistent with db. Removing from cache");
    delete(path);
    return null;
  }
  
  /**
   * Gets the INode details from the cache given the path
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public INode getINode(String path) {
    INode[] inodesDB = getINodesOnPath(path);
    // last entry is the inode
    if(inodesDB != null) {
     //System.out.println("Cache hit");
      return inodesDB[inodesDB.length-1];
    }
    return null;
  }

  /**
   * Deletes a key from the cache
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void delete(String key) {
    executor.execute(new CacheWriter(cache_writers[getNextWriterCache()], key, null, CacheWriter.DELETE_OP));
  }
  
  /**
   * Get all the cached children from INode file
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public List<INode> getChildren(long inodeId) {
    if(DFSConfigKeys.DFS_INODE_CACHE_ENABLED) {
      return (List<INode>)cache_readers[getNextReaderCache()].get(inodeId+"");
    }
    else {
      return null;
    }
  }
  
  /**
   * Caches the children related to INode file
   */
  public void putChildren(long inodeId, List<INode> children) {
    if(DFSConfigKeys.DFS_INODE_CACHE_ENABLED) {
      executor.execute(new CacheWriter(cache_writers[getNextWriterCache()], inodeId+"", children, CacheWriter.SET_OP));
    }
  }
  
  /**
   * Clears the cache of its data
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void clear() {
    for(int i=0; i< POOL_SIZE; i++) {
      cache_readers[i].flush();
      cache_writers[i].flush();
    }
  }
  
  /**
   * Path: /home/folder1/file1      <====> Inodes: 0,1,2,3
   * 
   *          /                 parent=" "               --> inodeId = 0, parentInode.id=-1
   *          home      parent=/                  --> inodeId = 1, parentInode.id=0
   *          folder1  parent=home       --> inodeId = 2, parentInode.id=1
   *          file1         parent=folder1   --> inodeId = 3, parentInode.id=2
   * 
   * The inodes fetched from database can come in any random order, so this is the problem in verifying
   * We have to match that 
   *        / =========> 0
   *        home =====> 1
   *        folder1 ===> 2
   *        file1 ======> 3
   * 
   * Supposing we have inodes in this order 3, 1, 0, 2
   * InodeTableSimple.id = 3, InodeTableSimple.name=file1, InodeTableSimple.parentId=2
   * This entry needs to be verified with /home/folder1/file1 from the path
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public boolean verify(ListOrderedMap<Long, INodeCachedEntry> cachedEntries, INode[] inodesDB) {
    //DBConnector.obtainSession().setPartitionKey(INodeTableSimple.class, cachedEntries.lastKey());
    return INodeHelper.verify(cachedEntries, inodesDB);
  }
  
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  public void close() {
    //cache.flush();
    for(int i=0; i< POOL_SIZE; i++) {
      cache_readers[i].shutdown(5000, TimeUnit.SECONDS);
      cache_writers[i].shutdown(5000, TimeUnit.SECONDS);
    }
    executor.shutdown();
    isConnected = false;
  }
  
  /**
   * Purpose of Cache writer is to provide asynchronous operations
   */
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  class CacheWriter implements Runnable{
    
    private MemcachedClient cache;
    private String key;
    private Object value;
    private int operation;
    
    public final static int DELETE_OP = 0;
    public final static int SET_OP = 1;
    
    CacheWriter(MemcachedClient cache, String key, Object value, int operation) {
      this.cache = cache;
      this.key = key;
      this.value = value;
      this.operation = operation;
    }
    
    @Override
    public void run() {
      switch(operation) {
        case DELETE_OP : cache.delete(key);
                                                   break;
        case SET_OP : cache.set(key, TIMEOUT, value);
                                                   break;
          
      }
    }
  }
}
