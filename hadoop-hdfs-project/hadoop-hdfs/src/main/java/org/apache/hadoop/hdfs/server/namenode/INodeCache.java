/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/** This interface provides methods for managing a local inode cache
 * @author wmalik
 *
 */
public interface INodeCache {
	
	/** Store a list of inodes in the cache
	 * @param inodes
	 */
	public void store(INode[] inodes);
	
	/** Get an array of INodeEntries from the cache
	 * @param path
	 * @return an array of INodeEntry if all path components exist in in the cache, and null otherwise 
	 */
	public INodeEntry[] get(String path);
	
	
	/** Get a fully cooked INode from the cache
	 * @param path
	 * @return
	 */
	public INode getNode(String path) throws PersistanceException;
	
	/** Initialize the rootNode in the cache (should be done at namenode startup)
	 * @param root
	 */
	public void putRoot(INodeDirectory root);
	
	/** Get the cache entry for root node
	 * @return
	 */
	public INodeEntry getRoot();
	
	/**Delete a path from the cache
	 * @param path
	 */
	public void delete(String path);	
	/**
	 * Clear the cache
	 */
	public void clear();
}
