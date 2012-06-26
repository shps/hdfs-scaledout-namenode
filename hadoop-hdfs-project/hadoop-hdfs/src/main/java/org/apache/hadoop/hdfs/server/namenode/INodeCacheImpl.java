/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.INodeFinder;

/** This class manages the local cache of INodes
 * @author wmalik
 * 
 */
public class INodeCacheImpl implements INodeCache {

	static final Log LOG = LogFactory.getLog(INodeCacheImpl.class);
	private INodeEntry rootEntry;
	private static INodeCacheImpl singletonCache = null;

	@Override
	public void store(INode[] inodes) {

		if (inodes.length == 0 || inodes == null || inodes[0] == null) {
			LOG.debug("inodes.length == 0 || inodes == null || inodes[0] == null");
			return;
		}
		if (!inodes[0].getName().equals("")) {
			LOG.debug("!inodes[0].getLocalName().equals(\"\")");
			return;
		}

		INodeEntry parentEntry = rootEntry;
		for (int i = 1; i < inodes.length; i++) {
			if (inodes[i] == null)
				break;
			INodeEntry entry = new INodeEntry(inodes[i].getId(),
					inodes[i].getNameBytes(), inodes[i].isDirectory());
			parentEntry.addChild(entry);
			parentEntry = parentEntry.getChildINode(entry.name);
		}

	}

	
	private boolean verify(List<INode> entries, byte[][] components) {
		
		if(entries.size() != components.length
				|| components.length == 0
        || entries.size() == 0)
			return false;

		for (int i = 0; i < entries.size(); i++) {
			String component = DFSUtil.bytes2String(components[i]);
			String inodeName = DFSUtil.bytes2String(entries.get(i).name);
			if(!component.equals(inodeName))
				return false;
		}
		
		return true;
	}
	
	@Override
	public INodeEntry[] get(String path) { //TODO: write a unit test to verify multiple scenarios
		byte[][] components = INodeDirectory.getPathComponents(path);
		INodeEntry[] inodes = new INodeEntry[components.length];
		long[] ids = new long[components.length];
		int count = getINodeCacheEntries(components, inodes, ids);

		int i = 0;
		for (; i < inodes.length; i++) {
			if (inodes[i] == null)
				break;
		}
		INodeEntry[] inodesWithoutNulls = Arrays.copyOfRange(inodes, 0, i);
		
		//DEBUG
//		for (int k = 0; k < inodesWithoutNulls.length; k++) {
//			if(inodesWithoutNulls[k] != null)
//				LOG.debug("From cache: " + DFSUtil.bytes2String(inodesWithoutNulls[k].name)
//						 + ":" + inodesWithoutNulls[k].id
//						);
//			else
//				LOG.debug("From cache: " + null);
//		}

		if(inodesWithoutNulls.length == components.length) {
			return inodesWithoutNulls;
		} else {
			return null;
		}
	}
	
	@Override
	public INode getNode(String path) {
		INodeEntry[] entries = get(path);

		if(entries != null) {
      List<Long> ids = toIds(entries);
      List<INode> inodes = (List<INode>) EntityManager.getInstance().findList(INodeFinder.ByIds, 
              ids);
      if (inodes.size() < ids.size()) //means the cache is old, so invalidate
        return null;
      List<INode> sortedList = sortINodesByPath(inodes);
			if(verify(sortedList, INodeDirectory.getPathComponents(path))) {
				LOG.debug("Cache Hit"); //TODO: Increment the cache miss metric here
				return sortedList.get(inodes.size()-1);
			}
			else { //cache invalidated
				//TODO
				//deleteChangedInodesFromCache(inodes, inodesFromCache)
				LOG.debug("Cache should be invalidated here");
			}
		}
		return null;
	}

	/** The implementation of this method has been shamelessly copied from INodeDirectory.getExistingPathINodes
	 * @param components
	 * @param existing
	 * @param ids
	 * @return count of existing inodes in cache
	 */
	private int getINodeCacheEntries(byte[][] components, INodeEntry[] existing,
			long[] ids) {
		// assert rootEntry.name == components[0] //TODO

		INodeEntry curNode = rootEntry;
		int count = 0;
		int eCount = 0;
		int index = existing.length - components.length;
		if (index > 0) {
			index = 0;
		}

		while (count < components.length && curNode != null) {
			final boolean lastComp = (count == components.length - 1);
			if (index >= 0) {
				existing[index] = curNode;
				ids[index] = curNode.id;
				eCount++;
			}
			if (lastComp || !curNode.isDir) {
				break;
			}
			INodeEntry parentDir = (INodeEntry) curNode;
			curNode = parentDir.getChildINode(components[count + 1]);
			count++;
			index++;
		}

		return eCount; //TODO: does this count nulls also?
	}
	
	/** This method deletes the cache entries of a path and should be called whenever 
	 *  a namespace change is detected 
	 * @param path
	 */
	@Override
	public void delete(String path) {
		//TODO
	}

	/**
	 * Initialize the cache entry for root
	 * @param root
	 */
	@Override
	public void putRoot(INodeDirectory root) {
		if (rootEntry == null)
			rootEntry = new INodeEntry(root.getId(), DFSUtil.string2Bytes(root
					.getName()), true);
	}

	@Override
	public INodeEntry getRoot() {
		return rootEntry;
	}

	public synchronized static INodeCacheImpl getInstance() {
		if (singletonCache == null) {
			singletonCache = new INodeCacheImpl();
		}
		return singletonCache;
	}

	private INodeCacheImpl() {}

	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}
	
	@SuppressWarnings("unused")
	private void printCache() {
		ArrayDeque<INodeEntry> q = new ArrayDeque<INodeEntry>();
		q.add(rootEntry);
		do {
			INodeEntry node = q.removeFirst();
			LOG.debug(node.id + ":" + DFSUtil.bytes2String(node.name));
			for (INodeEntry currChild : node.children) {
				q.add(currChild);
			}
		} while (!q.isEmpty());
	}

	@Override
	public void clear() {
		rootEntry = null; 
		singletonCache = null;
	}

  private List<Long> toIds(INodeEntry[] entries) {
    List<Long> ids = new ArrayList<Long>();
    for (INodeEntry iNodeEntry : entries) {
      ids.add(iNodeEntry.id);
    }
    return ids;
  }

  	/** Chain the inodes together according to the directory hierarchy
	 *  This is required because the ClusterJ "in" clause doesn't guarantee the order
	 *  
	 * @param inodeList a list of inodets which need to be sorted
	 * @return
	 */
	private List<INode> sortINodesByPath(List<INode> inodeList) {

		if (inodeList == null) {
			return null;
		}
		if (inodeList.size() == 1) {
			return inodeList;
		}

		//converting list to a map so that we can get an inode in O(1) during chaining
		Map<Long, INode> inodesMap = new HashMap<Long, INode>(); //<pid, inodetable>
		for (INode inode : inodeList) {
			inodesMap.put(inode.getParentId(), inode);
		}

		List<INode> sortedList =
				new ArrayList<INode>(inodeList.size());
		int count = 0;
		INode root = inodesMap.get(-1L); //use a constant here
		sortedList.add(root);
		Long next_parent_id = root.getId();

		//lets chain the inodes together
		while (count < inodeList.size() - 1) {
			INode inode = inodesMap.get(next_parent_id);
			if (inode == null) //if the chain is broken, return null and invalidate the cache
			{
				return null;
			}
			sortedList.add(inode);
			next_parent_id = inode.getId();
			count++;
		}
		return sortedList;
	}
        

}
