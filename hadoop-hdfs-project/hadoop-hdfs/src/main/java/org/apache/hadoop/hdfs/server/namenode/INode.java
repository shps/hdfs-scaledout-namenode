/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.util.StringUtils;

/**
 * We keep an in-memory representation of the file/block hierarchy. This is a
 * base INode class containing common fields for file and directory inodes.
 */
public abstract class INode implements Comparable<byte[]>, FSInodeInfo {

  public static enum Order implements Comparator<INode> {

    ByName() {

      @Override
      public int compare(INode o1, INode o2) {
        return o1.compareTo(o2.getNameBytes());
      }
    };

    @Override
    public abstract int compare(INode o1, INode o2);

    public Comparator acsending() {
      return this;
    }

    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }
  /*
   * The inode name is in java UTF8 encoding; The name in HdfsFileStatus should
   * keep the same encoding as this. if this encoding is changed, implicitly
   * getFileInfo and listStatus in clientProtocol are changed; The decoding at
   * the client side should change accordingly.
   */
  protected byte[] name;
  protected long id;
  protected long parentId;
  protected INodeDirectory parent;
  protected long modificationTime;
  protected long accessTime;

  /**
   * Simple wrapper for two counters : nsCount (namespace consumed) and dsCount
   * (diskspace consumed).
   */
  public static class DirCounts {

    long nsCount = 0;
    long dsCount = 0;

    /**
     * returns namespace count
     */
    long getNsCount() {
      return nsCount;
    }

    /**
     * returns diskspace count
     */
    long getDsCount() {
      return dsCount;
    }
  }
  // Only updated by updatePermissionStatus(...).
  // Other codes should not modify it.
  private long permission;

  private static enum PermissionStatusFormat {

    MODE(0, 16), GROUP(MODE.OFFSET + MODE.LENGTH, 25), USER(GROUP.OFFSET
    + GROUP.LENGTH, 23);
    final int OFFSET;
    final int LENGTH; // bit length
    final long MASK;

    PermissionStatusFormat(int offset, int length) {
      OFFSET = offset;
      LENGTH = length;
      MASK = ((-1L) >>> (64 - LENGTH)) << OFFSET;
    }

    long retrieve(long record) {
      return (record & MASK) >>> OFFSET;
    }

    long combine(long bits, long record) {
      return (record & ~MASK) | (bits << OFFSET);
    }
  }

  protected INode() {
    name = null;
    parent = null;
    modificationTime = 0;
    accessTime = 0;
  }

  INode(PermissionStatus permissions, long mTime, long atime) {
    this.name = null;
    this.parent = null;
    this.modificationTime = mTime;
    setAccessTime(atime);
    setPermissionStatus(permissions);
  }

  protected INode(String name, PermissionStatus permissions) {
    this(permissions, 0L, 0L);
    setName(name);
  }

  /**
   * copy constructor
   *
   * @param other Other node to be copied
   */
  public INode(INode other) {
    setName(other.getName());
    this.id = other.getId();
    this.parent = other.getParent();
    this.parentId = other.getParentId();
    this.accessTime = other.getAccessTime();
    setPermissionStatus(other.getPermissionStatus());
    setModificationTime(other.getModificationTime());
  }

  /**
   * Check whether this is the root inode.
   */
  public boolean isRoot() {
    return name.length == 0;
  }

  /**
   * Set the {@link PermissionStatus}
   */
  public final void setPermissionStatus(PermissionStatus ps) {
    setUser(ps.getUserName());
    setGroup(ps.getGroupName());
    setPermission(ps.getPermission());
  }

  /**
   * Get the {@link PermissionStatus}
   */
  public PermissionStatus getPermissionStatus() {
    return new PermissionStatus(getUserName(), getGroupName(),
            getFsPermission());
  }

  private synchronized void updatePermissionStatus(PermissionStatusFormat f,
          long n) {
    permission = f.combine(n, permission);
  }

  /**
   * Get user name
   */
  public String getUserName() {
    int n = (int) PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }

  /**
   * Set user
   */
  public void setUser(String user) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }

  /**
   * Get group name
   */
  public String getGroupName() {
    int n = (int) PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }

  /**
   * Set group
   */
  public void setGroup(String group) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }

  /**
   * Get the {@link FsPermission}
   */
  public FsPermission getFsPermission() {
    return new FsPermission(
            (short) PermissionStatusFormat.MODE.retrieve(permission));
  }

  public short getFsPermissionShort() {
    return (short) PermissionStatusFormat.MODE.retrieve(permission);
  }

  /**
   * Set the {@link FsPermission} of this {@link INode}
   */
  public void setPermission(FsPermission permission) {
    updatePermissionStatus(PermissionStatusFormat.MODE,
            permission.toShort());
  }

  /**
   * Check whether it's a directory
   */
  public abstract boolean isDirectory();

  public void setParent(INodeDirectory p) {
    this.parent = p;
  }

  /**
   * Collect all the blocks in all children of this INode. Count and return the
   * number of files in the sub tree. Also clears references since this INode is
   * deleted.
   */
  abstract int collectSubtreeBlocksAndClear(List<Block> blocks);

  /**
   * Compute {@link ContentSummary}.
   */
  public final ContentSummary computeContentSummary() {
    long[] a = computeContentSummary(new long[]{0, 0, 0, 0});
    return new ContentSummary(a[0], a[1], a[2], getNsQuota(), a[3],
            getDsQuota());
  }

  /**
   * @return an array of three longs. 0: length, 1: file count, 2: directory
   * count 3: disk space
   */
  abstract long[] computeContentSummary(long[] summary);

  /**
   * Get the quota set for this inode
   *
   * @return the quota if it is set; -1 otherwise
   */
  public long getNsQuota() {
    return -1;
  }

  public long getDsQuota() {
    return -1;
  }

  public boolean isQuotaSet() {
    return getNsQuota() >= 0 || getDsQuota() >= 0;
  }

  /**
   * Adds total number of names and total disk space taken under this tree to
   * counts. Returns updated counts object.
   */
  public abstract DirCounts spaceConsumedInTree(DirCounts counts);

  /**
   * Get local file name
   *
   * @return local file name
   */
  public String getName() {
    return DFSUtil.bytes2String(name);
  }

  public String getLocalParentDir() {
    INode inode = isRoot() ? this : getParent();
    return (inode != null) ? inode.getFullPathName() : "";
  }

  /**
   * Get local file name
   *
   * @return local file name
   */
  public byte[] getNameBytes() {
    return name;
  }

  /**
   * Set local file name
   */
  public final void setName(String name) {
    this.name = DFSUtil.string2Bytes(name);
  }

  /**
   * Set local file name
   */
  public final void setName(byte[] name) {
    this.name = name;
  }

  /**
   * {@inheritDoc}
   */
  public String getFullPathName() {
    return FSDirectory.getFullPathName(this);
  }

  /**
   * {@inheritDoc}
   */
  public String toString() {
    return "\"" + getFullPathName() + "\":" + getUserName() + ":"
            + getGroupName() + ":" + (isDirectory() ? "d" : "-")
            + getFsPermission();
  }

  /**
   * Get parent directory
   *
   * @return parent INode
   */
  public INodeDirectory getParent() {
    if (parent == null) {
      parent = (INodeDirectory) EntityManager.getInstance().findInodeById(getParentId());
    }
    return parent;
  }

  /**
   * Get last modification time of inode.
   *
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }

  /**
   * Set last modification time of inode.
   */
  public final void setModificationTime(long modtime) {
    assert isDirectory();
    if (this.modificationTime <= modtime) {
      this.modificationTime = modtime;
    }
  }

  /**
   * Always set the last modification time of inode.
   */
  public void setModificationTimeForce(long modtime) {
    assert !isDirectory();
    this.modificationTime = modtime;
  }

  /**
   * Get access time of inode.
   *
   * @return access time
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  public final void setAccessTime(long atime) {
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  public boolean isUnderConstruction() {
    return false;
  }

  /**
   * Check whether it's a symlink
   */
  public boolean isLink() {
    return false;
  }

  /**
   * Breaks file path into components.
   *
   * @param path
   * @return array of byte arrays each of which represents a single path
   * component.
   */
  public static byte[][] getPathComponents(String path) {
    return getPathComponents(getPathNames(path));
  }

  /**
   * Convert strings to byte arrays for path components.
   */
  public static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
      bytes[i] = DFSUtil.string2Bytes(strings[i]);
    }
    return bytes;
  }

  /**
   * Splits an absolute path into an array of path components.
   *
   * @param path
   * @throws AssertionError if the given path is invalid.
   * @return array of path components.
   */
  public static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      throw new AssertionError("Absolute path required");
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  /**
   * Given some components, create a path name.
   *
   * @param components The path components
   * @param start index
   * @param end index
   * @return concatenated path
   */
  protected static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }
  //
  // Comparable interface
  //

  @Override
  public int compareTo(byte[] o) {
    return compareBytes(name, o);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof INode)) {
      return false;
    }
    return Arrays.equals(this.name, ((INode) o).name);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.name);
  }

  //
  // static methods
  //
  /**
   * Compare two byte arrays.
   *
   * @return a negative integer, zero, or a positive integer as defined by
   *         {@link #compareTo(byte[])}.
   */
  public static int compareBytes(byte[] a1, byte[] a2) {
    if (a1 == a2) {
      return 0;
    }
    int len1 = (a1 == null ? 0 : a1.length);
    int len2 = (a2 == null ? 0 : a2.length);
    int n = Math.min(len1, len2);
    byte b1, b2;
    for (int i = 0; i < n; i++) {
      b1 = a1[i];
      b2 = a2[i];
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return len1 - len2;
  }

  public final void setId(long id) {
    this.id = id;
  }

  public long getId() {
    return this.id;
  }

  public void setParentId(long pid) {
    this.parentId = pid;
  }

  public long getParentId() {
    return this.parentId;
  }

  public String nameParentKey() {
    return parentId + getName();
  }
}
