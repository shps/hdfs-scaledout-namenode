package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.KetamaNodeLocator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 * MemcacheForINode tutorial
 * https://developers.google.com/appengine/articles/scaling/memcache
 * http://www.javaworld.com/javaworld/jw-05-2012/120515-memcached-for-java-enterprise-performance-2.html
 * [http://sacharya.com/using-memcached-with-java/]
 * [http://www.clusterdb.com/mysql-cluster/scalabale-persistent-ha-nosql-memcache-storage-using-mysql-cluster/]
 *
 * Pooled memcached servers can use consistent hashing in case one of them
 * fails, there will be some failover support. spy.memcached supports consistent
 * hashing. spymemcached is asynchronous - issue a store and continue processing
 * without having to wait for that operation to finish. We generally do blocking
 * gets.
 *
 * JIM: This singleton needs to be thread-safe. Each client gets their own
 * cache_reader or cacher_writer object. TODO: Need to synchronize
 * allocation/return of cache_reader and cacher_writer objects. Support for
 * encryption with memcached here:
 * http://code.google.com/p/spymemcached/wiki/Examples
 *
 *
 * Run memcache: memcached -uroot -m 1024 -d 127.0.0.1 -p 11211
 *
 */
public class MemcacheForINode {

    static final int TIMEOUT = 2000;
    static final Log LOG = LogFactory.getLog(MemcacheForINode.class);
    protected boolean isConnected = false;
    private static MemcacheForINode instance;
    protected MemcachedClient cache;
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------

    /**
     * Wrapper class for Future object returned by asynchronous get to 
     * Memcached. Clients can make blocking calls to getINodes from their
     * own thread with the returned object.
     * Example:
     * INode inodeFromCache = MemcacheForINode.getInstance().resolve(path).getINode();
     * 
     */
    public static class FutureInodes {

        private Future<Object> f;
        private String path;

        public FutureInodes(String path, Future<Object> f) {
            assert f != null;
            this.f = f;
            this.path = path;
        }

        public INode[] getINodesOnPath() throws PersistanceException 
        {
            ListOrderedMap<Long, INodeMemcachedEntry> inodes;
            try {
                ListOrderedMap<Long, INodeMemcachedEntry> cachedEntries = 
                        (ListOrderedMap<Long, INodeMemcachedEntry>) f.get(MemcacheForINode.TIMEOUT, TimeUnit.MILLISECONDS);
                if (cachedEntries == null || cachedEntries.isEmpty()) {
                    return null;
                }
                INode[] inodesDB = null;
                // if verified from DB to be correct, return the inode and save it in 'inodesDB'
                if (verify(cachedEntries, inodesDB)) {
                    //LOG.info("cache hit for path: ["+path+"]");
                    return inodesDB;
                }
            } catch (InterruptedException ex) {
                Logger.getLogger(INodeDirectory.class.getName()).log(Level.SEVERE, null, ex);
                f.cancel(false);
            } catch (ExecutionException ex) {
                Logger.getLogger(INodeDirectory.class.getName()).log(Level.SEVERE, null, ex);
                f.cancel(false);
            } catch (TimeoutException ex) {
                // Since we don't need this, go ahead and cancel the operation.  This
                // is not strictly necessary, but it'll save some work on the server.
                Logger.getLogger(INodeDirectory.class.getName()).log(Level.SEVERE, null, ex);
                f.cancel(false);
                // Do other timeout related stuff
            }
            // Cached Inode in Memcached is not correct. remove from cache
            LOG.debug("Path [" + path + "] not consistent with db. Removing from cache");
            MemcacheForINode.getInstance().delete(path);
            return null;
        }

        public INode getINode() throws PersistanceException
        {
            INode[] inodesDB = getINodesOnPath();
            // last entry is the inode
            if (inodesDB != null) {
                return inodesDB[inodesDB.length - 1];
            }
            return null;
        }

        private boolean verify(ListOrderedMap<Long, INodeMemcachedEntry> memcachedINodes, 
                INode[] inodesDB) throws PersistanceException {
            
            Long[] ids =  memcachedINodes.asList().toArray(new Long[memcachedINodes.size()]);
            Collection<INode> nodes = EntityManager.findList(INode.Finder.ByIds, (Object[]) ids);
            if (nodes == null | nodes.isEmpty()) {
                return false;
            }
            if (ids.length != nodes.size()) {
                return false;
            }
    
            // TODO - JIM: validate that the directories in the Collection are stored in the 
            // ascending order from root.
            Iterator<INode> iter = nodes.iterator();
            
            inodesDB = nodes.toArray(new INode[nodes.size()]);
            int j=0;
            for (INodeMemcachedEntry n : memcachedINodes.values()) {
                if (!iter.hasNext()) {
                    return false;
                }
                INode i = iter.next();
                // match the name and parent-id of all directories
                if (n.getName().compareTo(i.getName()) != 0 
                        || n.getParentid() != i.getParentId()) {
                    return false;
                }
            }
            return true;
        }
    }
    
    
    /**
     * Wrapper class for Future object returned by asynchronous get to 
     * Memcached. Clients can make blocking calls to getINodes from their
     * own thread with the returned object.
     * TODO: JIM Remove this. We shouldn't cache children!?!
     */
    public static class FutureChildren {

        private Future<Object> f;

        public FutureChildren(Future<Object> f) {
            assert f != null;
            this.f = f;
        }

        public List<INode> getChildren() throws PersistanceException 
        {
            try {
                List<INode> cachedChildren = 
                        (List<INode>) f.get(MemcacheForINode.TIMEOUT, TimeUnit.MILLISECONDS);
                if (cachedChildren == null || cachedChildren.isEmpty()) {
                    return null;
                }
                return cachedChildren;
            } catch (InterruptedException ex) {
                Logger.getLogger(INodeDirectory.class.getName()).log(Level.SEVERE, null, ex);
                f.cancel(false);
            } catch (ExecutionException ex) {
                Logger.getLogger(INodeDirectory.class.getName()).log(Level.SEVERE, null, ex);
                f.cancel(false);
            } catch (TimeoutException ex) {
                // Since we don't need this, go ahead and cancel the operation.  This
                // is not strictly necessary, but it'll save some work on the server.
                Logger.getLogger(INodeDirectory.class.getName()).log(Level.SEVERE, null, ex);
                f.cancel(false);
                // Do other timeout related stuff
            }
            return null;
        }
    }    

    private MemcacheForINode() {
    }

    public synchronized static MemcacheForINode getInstance() {
        if (instance == null) {
            instance = new MemcacheForINode();
        }
        return instance;
    }
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------

    public synchronized void setConfiguration(Configuration conf) throws IOException {
        if (!isConnected) {
            String[] servers = conf.get(DFSConfigKeys.DFS_MEMCACHE_SERVERS_KEY, DFSConfigKeys.DFS_MEMCACHE_SERVERS_DEFAULT).split(",");
            String serverAddresses = "";
            for (String s : servers) {
                serverAddresses += s + " ";
            }

            // Jim - changed to BinaryProtocol and consistent hashing (KETAMA_HASH)
            ConnectionFactory connectionFactory = new BinaryConnectionFactory(DefaultConnectionFactory.DEFAULT_OP_QUEUE_LEN,
                    DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE, DefaultHashAlgorithm.KETAMA_HASH) {
                @Override
                public NodeLocator createLocator(List<MemcachedNode> list) {
                    KetamaNodeLocator locator = new KetamaNodeLocator(list, DefaultHashAlgorithm.KETAMA_HASH);
                    return locator;
                }
            };
            cache = new MemcachedClient(connectionFactory, AddrUtil.getAddresses(serverAddresses.trim()));
            isConnected = true;
            LOG.info("Memcache client connected to server(s) [" + serverAddresses + "]");
        }
    }

    /**
     * I need to return the last inode from the data structure in O(1), but no
     * data structure to help me get it
     * [org.apache.commons.collections15.map.ListOrderedMap<K,V>]
     */
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    public void putINode(String path, ListOrderedMap<Long, INodeMemcachedEntry> inodesOnPath) {
        //LOG.debug("storing inodes in path ["+path.toString()+"] to cache");
        // set is an asynchronous call to xpymemcached.
        cache.set(path, 2000, inodesOnPath);
    }

    /**
     * gets the inode entries that lie on 'path' and verifies the entries are
     * correct via the db
     */
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    public FutureInodes resolve(String path) {
        Future<Object> f = cache.asyncGet(path);
        return f == null ? null : new FutureInodes(path, f);
    }

    /**
     * Deletes a key from the cache
     */
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    public void delete(String key) {
        // delete is an asynchronous call in spymemcached - don't need to launch a thread for this.
        cache.delete(key);
    }

    /**
     * Get all the cached children from INode file
     */
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    public FutureChildren resolveChildren(long parentId) {
        Future<Object> f = cache.asyncGet(Long.toString(parentId));
        return f == null ? null : new FutureChildren(f);
    }

    /**
     * Caches the children related to INode file
     */
    public void putChildren(long parentId, List<INode> children) {
            // MemcachedClient.set() is asynchronous. 
            cache.set(Long.toString(parentId), TIMEOUT, children);
    }

    /**
     * Clears the cache of its data
     */
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    public void clear() {
        cache.flush();
    }

    /**
     * Path: /home/folder1/file1 <====> Inodes: 0,1,2,3
     *
     *          / parent=" " --> inodeId = 0, parentInode.id=-1 home parent=/ -->
     * inodeId = 1, parentInode.id=0 folder1 parent=home --> inodeId = 2,
     * parentInode.id=1 file1 parent=folder1 --> inodeId = 3, parentInode.id=2
     *
     * The inodes fetched from database can come in any random order, so this is
     * the problem in verifying We have to match that / =========> 0 home =====>
     * 1 folder1 ===> 2 file1 ======> 3
     *
     * Supposing we have inodes in this order 3, 1, 0, 2 InodeTableSimple.id =
     * 3, InodeTableSimple.name=file1, InodeTableSimple.parentId=2 This entry
     * needs to be verified with /home/folder1/file1 from the path
     */

    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------
    public void close() {
        cache.shutdown(TIMEOUT * 2, TimeUnit.MILLISECONDS);
        isConnected = false;
    }
}
