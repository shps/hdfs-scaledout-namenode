package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;


public class INodeEntry implements Comparable<byte[]> {
	
	static final Log LOG = LogFactory.getLog(INodeEntry.class);
	
	long id;
	byte[] name;
	boolean isDir;
	INodeEntry parent;
	List<INodeEntry> children;

	public INodeEntry(long id, byte[] name, boolean isDir) {
		this.id = id;
		this.name = name;
		this.isDir = isDir;
		parent = null;
		children = new ArrayList<INodeEntry>();
	}

	public INodeEntry getChildINode(byte[] childName) {
		if (children == null) {
			return null;
		}
		int low = Collections.binarySearch(children, childName);
		if (low >= 0) {
			return children.get(low);
		}
		return null;
	}

	public INodeEntry addChild(INodeEntry node) {
		if (children == null) {
			children = new ArrayList<INodeEntry>();
		}
		int low = Collections.binarySearch(children, node.name);
		if(low >= 0) {
			INodeEntry exChild = children.get(low);
			String exName = DFSUtil.bytes2String(exChild.name);
			String nodeName = DFSUtil.bytes2String(node.name);
			if(exChild.id != node.id || !exName.equals(nodeName)) {
				LOG.debug("Updating INodeEntry from " + exChild.id + "->" + node.id +
						" " + exName  + " -> " + nodeName
						);
				exChild.name = node.name;
				exChild.id = node.id;
				exChild.isDir = node.isDir;
			}
			return null;
		}
		node.parent = this;
		children.add(-low - 1, node);
		LOG.debug("Added  "+ DFSUtil.bytes2String(this.name)+"->" + DFSUtil.bytes2String(node.name));
		return node;
	}

	@Override
	public int compareTo(byte[] o){
		return compareBytes(name, o);
	}


	/**
	 * Compare two byte arrays. (taken from INode class)
	 * 
	 * @return a negative integer, zero, or a positive integer as defined by
	 *         {@link #compareTo(byte[])}.
	 */
	int compareBytes(byte[] a1, byte[] a2) {
		if (a1 == a2)
			return 0;
		int len1 = (a1 == null ? 0 : a1.length);
		int len2 = (a2 == null ? 0 : a2.length);
		int n = Math.min(len1, len2);
		byte b1, b2;
		for (int i = 0; i < n; i++) {
			b1 = a1[i];
			b2 = a2[i];
			if (b1 != b2)
				return b1 - b2;
		}
		return len1 - len2;
	}
}