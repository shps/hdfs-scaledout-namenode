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

import com.mysql.clusterj.ClusterJException;
import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InodeDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.hdfs.util.ByteArray;

/**
 * ***********************************************
 * FSDirectory stores the filesystem directory state. It handles writing/loading
 * values to disk, and logging changes as we go.
 *
 * It keeps the filename->blockset mapping always-current and logged to disk.
 *
 ************************************************
 */
public class FSDirectory implements Closeable {

    
    private FSNamesystem fsNamesystem;
    private volatile boolean ready = false;
    public static final long UNKNOWN_DISK_SPACE = -1; //[S] name it public to access from FSnamesystem
    private final int maxComponentLength;
    private final int maxDirItems;
    private final int lsLimit;  // max list limit
    // lock to protect the directory and BlockMap
    private ReentrantReadWriteLock dirLock;
    private Condition cond;
    private boolean quotaEnabled;
    private String supergroup;  // need this to initialize the root inode
    public final static long ROOT_ID = 0L;
    public final static String ROOT = "";
    public final static long ROOT_PARENT_ID = -1L;

    // utility methods to acquire and release read lock and write lock
    void readLock() {
        if (FSNamesystem.isSystemLevelLockEnabled()) {
            this.dirLock.readLock().lock();
        }
    }

    void readUnlock() {
        if (FSNamesystem.isSystemLevelLockEnabled()) {
            this.dirLock.readLock().unlock();
        }
    }

    void writeLock() {
        if (FSNamesystem.isSystemLevelLockEnabled()) {
            this.dirLock.writeLock().lock();
        }
    }

    void writeUnlock() {
        if (FSNamesystem.isSystemLevelLockEnabled()) {
            this.dirLock.writeLock().unlock();
        }
    }

    boolean hasWriteLock() {
        if (!FSNamesystem.isSystemLevelLockEnabled()) {
            return true;
        }

        return this.dirLock.isWriteLockedByCurrentThread();
    }

    boolean hasReadLock() {
        if (!FSNamesystem.isSystemLevelLockEnabled()) {
            return true;
        }
        return this.dirLock.getReadHoldCount() > 0;
    }
    /**
     * Caches frequently used file names used in {@link INode} to reuse byte[]
     * objects and reduce heap usage.
     */
    private final NameCache<ByteArray> nameCache;

    FSDirectory(FSNamesystem ns, Configuration conf) {
        this.dirLock = new ReentrantReadWriteLock(true); // fair
        this.cond = dirLock.writeLock().newCondition();
        this.fsNamesystem = ns;
        supergroup = ns.getSupergroup();
        
        int configuredLimit = conf.getInt(
                DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
        this.lsLimit = configuredLimit > 0
                ? configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;

        // filesystem limits
        this.maxComponentLength = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
        this.maxDirItems = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);

        int threshold = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
                DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);

        this.quotaEnabled = conf.getBoolean(DFSConfigKeys.DFS_QUOTA_ENABLED_KEY,
                DFSConfigKeys.DFS_QUOTA_ENABLED_KEY_DEFAULT);

        NameNode.LOG.info("Caching file names occuring more than " + threshold
                + " times ");
        nameCache = new NameCache<ByteArray>(threshold);
    }

    private FSNamesystem getFSNamesystem() {
      return this.fsNamesystem;
    }

    private BlockManager getBlockManager() {
        return getFSNamesystem().getBlockManager();
    }

    /**
     * Load the filesystem image into memory.
     *
     * @param startOpt Startup type as specified by the user.
     * @throws IOException If image or editlog cannot be read.
     */
    void loadFSImage(StartupOption startOpt)
            throws IOException {
        // format before starting up if requested
        if (startOpt == StartupOption.FORMAT) {
            startOpt = StartupOption.REGULAR;
        }
        writeLock();
        try {
            setReady(true);
            this.nameCache.initialized();
            if (FSNamesystem.isSystemLevelLockEnabled()) {
                cond.signalAll();
            }
        } finally {
            writeUnlock();
        }
    }

    // exposed for unit tests
    protected void setReady(boolean flag) {
        ready = flag;
    }

    private void incrDeletedFileCount(int count) {
        if (getFSNamesystem() != null) {
            NameNode.getNameNodeMetrics().incrFilesDeleted(count);
        }
    }

    /**
     * Shutdown the filestore
     */
    public void close() throws IOException {
    }

    /**
     * Block until the object is ready to be used.
     */
    void waitForReady() {
        if (FSNamesystem.isSystemLevelLockEnabled()) {
            if (!ready) {
                writeLock();
                try {
                    while (!ready) {
                        try {
                            cond.await(5000, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException ie) {
                        }
                    }
                } finally {
                    writeUnlock();
                }
            }
        }
    }

    /**
     * Add the given filename to the fs.
     *
     * @throws QuotaExceededException
     * @throws FileAlreadyExistsException
     */
    INodeFile addFile(String path,
            PermissionStatus permissions,
            short replication,
            long preferredBlockSize,
            String clientName,
            String clientMachine,
            DatanodeID clientNode,
            long generationStamp,
            INodeFile existingInode)
            throws FileAlreadyExistsException, QuotaExceededException,
            UnresolvedLinkException, PersistanceException, IOException{
        waitForReady();

        // Always do an implicit mkdirs for parent directory tree.
        long modTime = now();
        if (!mkdirs(new Path(path).getParent().toString(), permissions, true,
                modTime)) {
            return null;
        }

        boolean reuseId = existingInode != null;
        INodeFile newNode;
        if (existingInode != null)
        {
          newNode = existingInode;
          newNode.convertToUnderConstruction(clientName, clientMachine, clientNode);
        } 
        else
          newNode = new INodeFile(true,
                permissions, replication,
                preferredBlockSize, modTime, clientName,
                clientMachine, clientNode);
        writeLock();
        try {
            newNode = addNode(path, newNode, UNKNOWN_DISK_SPACE, false, reuseId);
        } finally {
            writeUnlock();
        }
        if (newNode == null) {
            NameNode.stateChangeLog.info("DIR* FSDirectory.addFile: "
                    + "failed to add " + path
                    + " to the file system");
            return null;
        }
        // add create file record to log, record new generation stamp

        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.addFile: "
                    + path + " is added to the file system");
        }
        return newNode;
    }

    /**
     */
    INode unprotectedAddFile(String path,
            PermissionStatus permissions,
            BlockInfo[] blocks,
            short replication,
            long modificationTime,
            long atime,
            long preferredBlockSize)
            throws UnresolvedLinkException, PersistanceException {
        INode newNode;
        long diskspace = UNKNOWN_DISK_SPACE;
        assert hasWriteLock();
        if (blocks == null) {
            newNode = new INodeDirectory(permissions, modificationTime);
        } else {
            newNode = new INodeFile(false, permissions, replication,
                    modificationTime, atime, preferredBlockSize);
            diskspace = ((INodeFile) newNode).diskspaceConsumed(blocks);
        }
        writeLock();
        try {
            try {
                newNode = addNode(path, newNode, diskspace, false);
                if (newNode != null && blocks != null) {
                    int nrBlocks = blocks.length;
                    // Add file->block mapping
                    INodeFile newF = (INodeFile) newNode;
                    List<BlockInfo> blks = new ArrayList<BlockInfo>();
                    for (int i = 0; i < nrBlocks; i++) {
                        blocks[i].setINode(newF);
                        blks.add(blocks[i]);
                        EntityManager.add(blocks[i]);
                    }
                    newF.setBlocks(blks);

                }
            } catch (IOException e) {
                return null;
            }
            return newNode;
        } finally {
            writeUnlock();
        }

    }

    INodeDirectory addToParent(byte[] src, INodeDirectory parentINode,
            INode newNode, boolean propagateModTime) throws IOException, PersistanceException {
        // NOTE: This does not update space counts for parents
        INodeDirectory newParent = null;
        writeLock();
        try {
            try {
                newParent = getRootDir().addToParent(src, newNode, parentINode,
                        false, propagateModTime);
                if (newParent != null) {
                    EntityManager.update(newNode);
                    EntityManager.update(newParent);
                }
                cacheName(newNode);
            } catch (FileNotFoundException e) {
                return null;
            }
            if (newParent == null) {
                return null;
            }
        } finally {
            writeUnlock();
        }
        return newParent;
    }

    /**
     * Add a block to the file. Returns a reference to the added block.
     *
     * @throws IOException
     */
    BlockInfo addBlock(String path,
            INode[] inodes,
            Block block,
            DatanodeDescriptor targets[]) throws IOException, PersistanceException {
        waitForReady();

        writeLock();
        try {
            assert inodes[inodes.length - 1].isUnderConstruction() :
                    "INode should correspond to a file under construction";
            INodeFile fileINode =
                    (INodeFile) inodes[inodes.length - 1];

            assert fileINode.isUnderConstruction();

            // check quota limits and updated space consumed
            updateCount(inodes, inodes.length - 1, 0,
                    fileINode.getPreferredBlockSize() * fileINode.getReplication(), true);

            BlockInfoUnderConstruction blockInfo =
                    new BlockInfoUnderConstruction(
                    block);
            blockInfo.setBlockUCState(BlockUCState.UNDER_CONSTRUCTION);
            for (DatanodeDescriptor dn : targets) {
                ReplicaUnderConstruction expReplica = blockInfo.addFirstExpectedReplica(dn.getStorageID(), HdfsServerConstants.ReplicaState.RBW);
                if (expReplica != null) {
                    EntityManager.add(expReplica);
                }
            }
            blockInfo.setINode(fileINode);
            fileINode.addBlock(blockInfo);
            EntityManager.add(blockInfo);
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.addBlock: "
                        + path + " with " + block
                        + " block is added to the in-memory "
                        + "file system");
            }
            return blockInfo;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Persist the block list for the inode.
     *
     * @throws IOException
     */
    void persistBlocks(String path, INodeFile file) throws IOException, PersistanceException {
        assert file.isUnderConstruction();
        waitForReady();

        writeLock();
        try {
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.persistBlocks: "
                        + path + " with " + file.getBlocks().size()
                        + " blocks is persisted to the file system");
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Close file.
     *
     * @throws IOException
     */
    void closeFile(String path, INodeFile file) throws IOException, PersistanceException {
        waitForReady();
        long now = now();
        writeLock();
        try {
            // file is closed
            file.setModificationTimeForce(now);
            EntityManager.update(file);
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.closeFile: "
                        + path + " with " + file.getBlocks().size()
                        + " blocks is persisted to the file system");
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Remove a block to the file.
     */
    boolean removeBlock(String path, INodeFile fileNode,
            Block block) throws IOException, PersistanceException {
        assert fileNode.isUnderConstruction();
        waitForReady();

        writeLock();
        try {
            // modify file-> block and blocksMap
            getBlockManager().removeBlockFromMap(block);
            // write modified block locations to log
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
                        + path + " with " + block
                        + " block is removed from the file system");
            }

            // update space consumed
            INode[] pathINodes = getExistingPathINodes(path);
            updateCount(pathINodes, pathINodes.length - 1, 0,
                    -fileNode.getPreferredBlockSize() * fileNode.getReplication(), true);
        } finally {
            writeUnlock();
        }
        return true;
    }

    /**
     * @see #unprotectedRenameTo(String, String, long)
     * @deprecated Use {@link #renameTo(String, String, Rename...)} instead.
     */
    @Deprecated
    boolean renameTo(String src, String dst)
            throws QuotaExceededException, UnresolvedLinkException,
            FileAlreadyExistsException, PersistanceException,IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
                    + src + " to " + dst);
        }
        waitForReady();
        long now = now();
        writeLock();
        try {
            if (!unprotectedRenameTo(src, dst, now)) {
                return false;
            }
        } finally {
            writeUnlock();
        }
        return true;
    }

    /**
     * @see #unprotectedRenameTo(String, String, long, Options.Rename...)
     */
    void renameTo(String src, String dst, Options.Rename... options)
            throws FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, QuotaExceededException,
            UnresolvedLinkException, IOException, PersistanceException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src
                    + " to " + dst);
        }
        waitForReady();
        long now = now();
        writeLock();
        try {
            if (unprotectedRenameTo(src, dst, now, options)) {
                incrDeletedFileCount(1);
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Change a path name
     *
     * @param src source path
     * @param dst destination path
     * @return true if rename succeeds; false otherwise
     * @throws QuotaExceededException if the operation violates any quota limit
     * @throws FileAlreadyExistsException if the src is a symlink that points to
     * dst
     * @deprecated See {@link #renameTo(String, String)}
     */
    @Deprecated
    boolean unprotectedRenameTo(String src, String dst, long timestamp)
            throws QuotaExceededException, UnresolvedLinkException,
            FileAlreadyExistsException, PersistanceException,IOException {
        assert hasWriteLock();
        INode[] srcInodes = getRootDir().getExistingPathINodes(src, false);
        for (int i = 0; i < srcInodes.length; i++) {
            NameNode.LOG.debug("srcInodes: " + srcInodes[i]);
        }
        INode srcInode = srcInodes[srcInodes.length - 1];

        // check the validation of the source
        if (srcInode == null) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + "failed to rename " + src + " to " + dst
                    + " because source does not exist");
            return false;
        }
        if (srcInodes.length == 1) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + "failed to rename " + src + " to " + dst + " because source is the root");
            return false;
        }
        if (isDir(dst)) {
            dst += Path.SEPARATOR + new Path(src).getName();
        }

        // check the validity of the destination
        if (dst.equals(src)) {
            return true;
        }
        if (srcInode.isLink()
                && dst.equals(((INodeSymlink) srcInode).getLinkValue())) {
            throw new FileAlreadyExistsException(
                    "Cannot rename symlink " + src + " to its target " + dst);
        }

        // dst cannot be directory or a file under src
        if (dst.startsWith(src)
                && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + "failed to rename " + src + " to " + dst
                    + " because destination starts with src");
            return false;
        }

        byte[][] dstComponents = INode.getPathComponents(dst);
        INode[] dstInodes = new INode[dstComponents.length];
        getRootDir().getExistingPathINodes(dstComponents, dstInodes, false);
        if (dstInodes[dstInodes.length - 1] != null) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + "failed to rename " + src + " to " + dst
                    + " because destination exists");
            return false;
        }
        if (dstInodes[dstInodes.length - 2] == null) {
            NameNode.stateChangeLog.warn("DIRsession.savePersistent(result);* FSDirectory.unprotectedRenameTo: "
                    + "failed to rename " + src + " to " + dst
                    + " because destination's parent does not exist");
            return false;
        }

        // Ensure dst has quota to accommodate rename
        if (isQuotaEnabled())
        {
            verifyQuotaForRename(srcInodes, dstInodes);
        }
        
        INode dstChild = null;
        INode srcChild = null;
        String srcChildName = null;
        try {
            srcChild = removeChild(srcInodes, srcInodes.length-1);
            if (srcChild == null) {
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        + "failed to rename " + src + " to " + dst
                        + " because the source can not be removed");
                return false;
            }
            srcChildName = srcChild.getName();
            srcChild.setName(dstComponents[dstInodes.length - 1]);
            // add src to the destination
            dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1,
                    srcChild, UNKNOWN_DISK_SPACE, false, true);

            if (dstChild != null) {
                srcChild = null;
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: "
                            + src + " is renamed to " + dst);
                }
                // update modification time of dst and the parent of src
                srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
                dstInodes[dstInodes.length - 2].setModificationTime(timestamp);

                EntityManager.update(srcInodes[srcInodes.length - 2]);
                EntityManager.update(dstInodes[dstInodes.length - 2]);
                return true;
            }
            else 
            {
                 NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                + "failed to rename " + src + " to " + dst+
                          " dstChild is null");
            }
        } finally {
            if (dstChild == null && srcChild != null) {
                // put it back
                srcChild.setName(srcChildName);
                addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, srcChild,
                        UNKNOWN_DISK_SPACE, false, true);
            }
        }
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                + "failed to rename " + src + " to " + dst);
        return false;
    }

    /**
     * Rename src to dst. See
     * {@link DistributedFileSystem#rename(Path, Path, Options.Rename...)} for
     * details related to rename semantics and exceptions.
     *
     * @param src source path
     * @param dst destination path
     * @param timestamp modification time
     * @param options Rename options
     */
    boolean unprotectedRenameTo(String src, String dst, long timestamp, Options.Rename... options) throws FileAlreadyExistsException,
            FileNotFoundException, ParentNotDirectoryException,
            QuotaExceededException, UnresolvedLinkException, IOException, PersistanceException {
        assert hasWriteLock();
        boolean overwrite = false;
        if (null != options) {
            for (Rename option : options) {
                if (option == Rename.OVERWRITE) {
                    overwrite = true;
                }
            }
        }
        String error = null;
        final INode[] srcInodes = getRootDir().getExistingPathINodes(src, false);
        final INode srcInode = srcInodes[srcInodes.length - 1];
        // validate source
        if (srcInode == null) {
            error = "rename source " + src + " is not found.";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new FileNotFoundException(error);
        }
        if (srcInodes.length == 1) {
            error = "rename source cannot be the root";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }

        // validate the destination
        if (dst.equals(src)) {
            throw new FileAlreadyExistsException(
                    "The source " + src + " and destination " + dst + " are the same");
        }
        if (srcInode.isLink()
                && dst.equals(((INodeSymlink) srcInode).getLinkValue())) {
            throw new FileAlreadyExistsException(
                    "Cannot rename symlink " + src + " to its target " + dst);
        }
        // dst cannot be a directory or a file under src
        if (dst.startsWith(src)
                && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
            error = "Rename destination " + dst
                    + " is a directory or file under source " + src;
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }
        final byte[][] dstComponents = INode.getPathComponents(dst);
        final INode[] dstInodes = new INode[dstComponents.length];
        getRootDir().getExistingPathINodes(dstComponents, dstInodes, false);
        INode dstInode = dstInodes[dstInodes.length - 1];
        if (dstInodes.length == 1) {
            error = "rename destination cannot be the root";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }
        if (dstInode != null) { // Destination exists
            // It's OK to rename a file to a symlink and vice versa
            if (dstInode.isDirectory() != srcInode.isDirectory()) {
                error = "Source " + src + " and destination " + dst
                        + " must both be directories";
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        + error);
                throw new IOException(error);
            }
            if (!overwrite) { // If destination exists, overwrite flag must be true
                error = "rename destination " + dst + " already exists";
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        + error);
                throw new FileAlreadyExistsException(error);
            }
            List<INode> children = dstInode.isDirectory()
                    ? ((INodeDirectory) dstInode).getChildren() : null;
            if (children != null && children.size() != 0) {
                error = "rename cannot overwrite non empty destination directory "
                        + dst;
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        + error);
                throw new IOException(error);
            }
        }
        if (dstInodes[dstInodes.length - 2] == null) {
            error = "rename destination parent " + dst + " not found.";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new FileNotFoundException(error);
        }
        if (!dstInodes[dstInodes.length - 2].isDirectory()) {
            error = "rename destination parent " + dst + " is a file.";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new ParentNotDirectoryException(error);
        }

        // Ensure dst has quota to accommodate rename
        verifyQuotaForRename(srcInodes, dstInodes);
        //INode removedSrc = removeChild(srcInodes, srcInodes.length - 1);
        INode removedSrc = srcInodes[srcInodes.length - 1];
        if (removedSrc == null) {
            error = "Failed to rename " + src + " to " + dst
                    + " because the source can not be removed";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }
        final String srcChildName = removedSrc.getName();
        String dstChildName = null;
        INode removedDst = null;
        try {
            if (dstInode != null) { // dst exists remove it
                removedDst = removeChild(dstInodes, dstInodes.length - 1);//[H]: We should remove dst here.
//        removedDst = dstInodes[dstInodes.length-1];
                dstChildName = removedDst.getName();
            }

            INode dstChild = null;
            removedSrc.setName(dstComponents[dstInodes.length - 1]);
            // add src as dst to complete rename
            dstChild = addChildNoQuotaCheck(dstInodes, dstInodes.length - 1,
                    removedSrc, UNKNOWN_DISK_SPACE, false, true);

            int filesDeleted = 0;
            if (dstChild != null) {
                removedSrc = null;
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug(
                            "DIR* FSDirectory.unprotectedRenameTo: " + src
                            + " is renamed to " + dst);
                }
                srcInodes[srcInodes.length - 2].setModificationTime(timestamp);
                dstInodes[dstInodes.length - 2].setModificationTime(timestamp);

                // Collect the blocks and remove the lease for previous dst
                if (removedDst != null) {
                    INode rmdst = removedDst;
                    removedDst = null;
                    List<Block> collectedBlocks = new ArrayList<Block>();

                    if (rmdst instanceof INodeDirectory) {
                        filesDeleted = rmdst.collectSubtreeBlocksAndClear(collectedBlocks);
                    } else if (rmdst instanceof INodeFile) {
                        filesDeleted = ((INodeFile) rmdst).collectSubtreeBlocksAndClearNoDelete(collectedBlocks);
                    }

                    getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
                }
                return filesDeleted > 0;
            }
        } catch (ClusterJException e) {
            throw e;
        } finally {
            //[H]: This is unnecessary in transaction. It rolls back in case of failure. 
//      if (removedSrc != null) {
//        // Rename failed - restore src
//        removedSrc.setLocalName(srcChildName);
//        addChildNoQuotaCheck(srcInodes, srcInodes.length - 1, removedSrc, 
//            UNKNOWN_DISK_SPACE, false, true, isTransactional);
//      }
//      if (removedDst != null) {
//        // Rename failed - restore dst
//        removedDst.setLocalName(dstChildName);
//        addChildNoQuotaCheck(dstInodes, dstInodes.length - 1, removedDst, 
//            UNKNOWN_DISK_SPACE, false, true, isTransactional);
//      }
        }
        NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                + "failed to rename " + src + " to " + dst);
        throw new IOException("rename from " + src + " to " + dst + " failed.");
    }

    /**
     * Set file replication
     *
     * @param src file name
     * @param replication new replication
     * @param oldReplication old replication - output parameter
     * @return array of file blocks
     * @throws IOException
     */
    List<BlockInfo> setReplication(String src, short replication, short[] oldReplication)
            throws IOException, PersistanceException {
        waitForReady();
        List<BlockInfo> fileBlocks = null;
        writeLock();
        try {
            fileBlocks = unprotectedSetReplication(src, replication, oldReplication);
            return fileBlocks;
        } finally {
            writeUnlock();
        }
    }

    List<BlockInfo> unprotectedSetReplication(String src,
            short replication,
            short[] oldReplication) throws IOException, PersistanceException {
        assert hasWriteLock();

        INode[] inodes = getRootDir().getExistingPathINodes(src, true);
        INode inode = inodes[inodes.length - 1];
        if (inode == null) {
            return null;
        }
        assert !inode.isLink();
        if (inode.isDirectory()) {
            return null;
        }
        INodeFile fileNode = (INodeFile) inode;
        final short oldRepl = fileNode.getReplication();

        // check disk quota
        long dsDelta = (replication - oldRepl) * (fileNode.diskspaceConsumed() / oldRepl);
        updateCount(inodes, inodes.length - 1, 0, dsDelta, true);

        fileNode.setReplication(replication);
        EntityManager.update(fileNode);

        if (oldReplication != null) {
            oldReplication[0] = oldRepl;
        }
        return fileNode.getBlocks();
    }

    /**
     * Get the blocksize of a file
     *
     * @param filename the filename
     * @return the number of bytes
     */
    long getPreferredBlockSize(String filename) throws UnresolvedLinkException,
            FileNotFoundException, IOException, PersistanceException {
        readLock();
        try {
            INode inode = getRootDir().getNode(filename, false);
            if (inode == null) {
                throw new FileNotFoundException("File does not exist: " + filename);
            }
            if (inode.isDirectory() || inode.isLink()) {
                throw new IOException("Getting block size of non-file: " + filename);
            }
            return ((INodeFile) inode).getPreferredBlockSize();
        } finally {
            readUnlock();
        }
    }

    boolean exists(String src) throws IOException, PersistanceException {
        src = normalizePath(src);
        readLock();
        try {
            INode inode = getRootDir().getNode(src, false);
            if (inode == null) {
                return false;
            }
            return inode.isDirectory() || inode.isLink()
                    ? true
                    : ((INodeFile) inode).getBlocks() != null;
        } finally {
            readUnlock();
        }
    }

    void setPermission(String src, FsPermission permission)
            throws FileNotFoundException, UnresolvedLinkException, IOException, PersistanceException {
        writeLock();
        try {
            unprotectedSetPermission(src, permission);
        } finally {
            writeUnlock();
        }
    }

    void unprotectedSetPermission(String src, FsPermission permissions)
            throws FileNotFoundException, UnresolvedLinkException, PersistanceException, IOException {
        assert hasWriteLock();
        INode inode = getRootDir().getNode(src, true);
        if (inode == null) {
            throw new FileNotFoundException("File does not exist: " + src);
        }
        inode.setPermission(permissions);
        EntityManager.update(inode);
    }

    void setOwner(String src, String username, String groupname)
            throws FileNotFoundException, UnresolvedLinkException, IOException, PersistanceException {
        writeLock();
        try {
            unprotectedSetOwner(src, username, groupname);
        } finally {
            writeUnlock();
        }
    }

    void unprotectedSetOwner(String src, String username, String groupname)
            throws FileNotFoundException, UnresolvedLinkException, IOException, PersistanceException {
        assert hasWriteLock();
        INode inode = getRootDir().getNode(src, true);
        if (inode == null) {
            throw new FileNotFoundException("File does not exist: " + src);
        }

        if (username != null) {
            inode.setUser(username);
        }
        if (groupname != null) {
            inode.setGroup(groupname);
        }

        EntityManager.update(inode);
    }

    /**
     * Concat all the blocks from srcs to trg and delete the srcs files
     *
     * @throws IOException
     */
    public void concat(String target, String[] srcs)
            throws IOException, PersistanceException {
        writeLock();
        try {
            // actual move
            waitForReady();
            long timestamp = now();
            unprotectedConcat(target, srcs, timestamp);
            // do the commit
        } finally {
            writeUnlock();
        }
    }

    /**
     * Concat all the blocks from srcs to trg and delete the srcs files
     *
     * @param target target file to move the blocks to
     * @param srcs list of file to move the blocks from Must be public because
     * also called from EditLogs NOTE: - it does not update quota (not needed
     * for concat)
     * @throws IOException
     */
    public void unprotectedConcat(String target, String[] srcs, long timestamp)
            throws IOException, PersistanceException {
        assert hasWriteLock();
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to " + target);
        }
        // do the move

        INode[] trgINodes = getExistingPathINodes(target);
        INodeFile trgInode = (INodeFile) trgINodes[trgINodes.length - 1];
        INodeDirectory trgParent = (INodeDirectory) trgINodes[trgINodes.length - 2];

        for (String src : srcs) {
            INodeFile srcInode = getFileINode(src);

            for (BlockInfo block : srcInode.getBlocks()) {
                trgInode.addBlock(block);
                block.setINode(trgInode);
                EntityManager.update(block);
            }
            srcInode.getBlocks().clear();
            INode removeChild = trgParent.removeChild(srcInode);
            EntityManager.remove(removeChild);
        }

        //TODO[Hooman]: The following changes can be optimized to be updated or flushed together.
        trgInode.setModificationTimeForce(timestamp);
        EntityManager.update(trgInode);
        trgParent.setModificationTime(timestamp);
        EntityManager.update(trgParent);
        // update quota on the parent directory ('count' files removed, 0 space)
        if (quotaEnabled) {
            unprotectedUpdateCount(trgINodes, trgINodes.length - 1, -srcs.length, 0);
        }
    }

    /**
     * Delete the target directory and collect the blocks under it
     *
     * @param src Path of a directory to delete
     * @param collectedBlocks Blocks under the deleted directory
     * @return true on successful deletion; else false
     * @throws IOException
     */
    boolean delete(String src, List<Block> collectedBlocks)
            throws IOException, PersistanceException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
        }
        waitForReady();
        long now = now();
        int filesRemoved;
        writeLock();
        try {
            filesRemoved = unprotectedDelete(src, collectedBlocks, now);
        } finally {
            writeUnlock();
        }
        if (filesRemoved <= 0) {
            return false;
        }
        incrDeletedFileCount(filesRemoved);
        // Blocks will be deleted later by the caller of this method
        getFSNamesystem().removePathAndBlocks(src, null);
        return true;
    }

    /**
     * Return if a directory is empty or not *
     */
    boolean isDirEmpty(String src) throws UnresolvedLinkException, PersistanceException, IOException {
        boolean dirNotEmpty = true;
        if (!isDir(src)) {
            return true;
        }
        readLock();
        try {
            INode targetNode = getRootDir().getNode(src, false);
            assert targetNode != null : "should be taken care in isDir() above";
            if (((INodeDirectory) targetNode).getChildren().size() != 0) {
                dirNotEmpty = false;
            }
        } finally {
            readUnlock();
        }
        return dirNotEmpty;
    }

    boolean isEmpty() throws PersistanceException, IOException {
        try {
            return isDirEmpty("/");
        } catch (UnresolvedLinkException e) {
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("/ cannot be a symlink");
            }
            assert false : "/ cannot be a symlink";
            return true;
        }
    }

    /**
     * Delete a path from the name space Update the count at each ancestor
     * directory with quota <br> Note: This is to be used by {@link FSEditLog}
     * only. <br>
     *
     * @param src a string representation of a path to an inode
     * @param mtime the time the inode is removed
     * @throws IOException
     */
    void unprotectedDelete(String src, long mtime)
            throws IOException, PersistanceException {
        assert hasWriteLock();
        List<Block> collectedBlocks = new ArrayList<Block>();
        int filesRemoved = unprotectedDelete(src, collectedBlocks, mtime);
        if (filesRemoved > 0) {
            getFSNamesystem().removePathAndBlocks(src, collectedBlocks);
        }
    }

    /**
     * Delete a path from the name space Update the count at each ancestor
     * directory with quota
     *
     * @param src a string representation of a path to an inode
     * @param collectedBlocks blocks collected from the deleted path
     * @param mtime the time the inode is removed
     * @return the number of inodes deleted; 0 if no inodes are deleted.
     */
    int unprotectedDelete(String src, List<Block> collectedBlocks,
            long mtime) throws UnresolvedLinkException, PersistanceException, IOException {
        assert hasWriteLock();
        src = normalizePath(src);

        INode[] inodes = getRootDir().getExistingPathINodes(src, false);
        INode targetNode = inodes[inodes.length - 1];


        if (targetNode == null) { // non-existent src
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                        + "failed to remove " + src + " because it does not exist");
            }
            return 0;
        }
        if (inodes.length == 1) { // src is the root
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: "
                    + "failed to remove " + src
                    + " because the root is not allowed to be deleted");
            return 0;
        }
        int pos = inodes.length - 1;

        // Remove the node from the namespace
        targetNode = removeChild(inodes, pos);
        if (targetNode == null) {
            return 0;
        }
        // set the parent's modification time
        inodes[pos - 1].setModificationTime(mtime);
        EntityManager.update(inodes[pos - 1]);

        int filesRemoved = 1;
        if (targetNode instanceof INodeDirectory || targetNode instanceof INodeDirectoryWithQuota) {
            filesRemoved = targetNode.collectSubtreeBlocksAndClear(collectedBlocks);
        } else if (targetNode instanceof INodeFile)//since we have already deleted the inode, we just need to clear the blocks
        {
            filesRemoved = ((INodeFile) targetNode).collectSubtreeBlocksAndClearNoDelete(collectedBlocks);
        }
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                    + src + " is removed");
        }
        return filesRemoved;

    }

    /**
     * Get a partial listing of the indicated directory
     *
     * @param src the directory name
     * @param startAfter the name to start listing after
     * @param needLocation if block locations are returned
     * @return a partial listing starting after startAfter
     */
    DirectoryListing getListing(String src, byte[] startAfter,
            boolean needLocation) throws UnresolvedLinkException, IOException, PersistanceException {
        String srcs = normalizePath(src);

        readLock();
        try {
            INode targetNode = getRootDir().getNode(srcs, true);

            if (targetNode == null) {
                return null;
            }


            if (!targetNode.isDirectory()) {
                return new DirectoryListing(
                        new HdfsFileStatus[]{createFileStatus(HdfsFileStatus.EMPTY_NAME,
                            targetNode, needLocation)}, 0);
            }

            // Else its a directory
            INodeDirectory dirInode = (INodeDirectory) targetNode;
            List<INode> contents = dirInode.getChildren();
            int startChild = dirInode.nextChild(startAfter, contents);
            int totalNumChildren = contents.size();
            int numOfListing = Math.min(totalNumChildren - startChild, this.lsLimit);
            HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
            for (int i = 0; i < numOfListing; i++) {
                INode cur = contents.get(startChild + i);
                listing[i] = createFileStatus(cur.name, cur, needLocation);
            }
            return new DirectoryListing(
                    listing, totalNumChildren - startChild - numOfListing);
        } finally {
            readUnlock();
        }
    }

    /**
     * Get the file info for a specific file.
     *
     * @param src The string representation of the path to the file
     * @param resolveLink whether to throw UnresolvedLinkException
     * @return object containing information regarding the file or null if file
     * not found
     */
    HdfsFileStatus getFileInfo(String src, boolean resolveLink)
            throws UnresolvedLinkException, PersistanceException, IOException {
        String srcs = normalizePath(src);
        readLock();
        try {
            INode targetNode = getRootDir().getNode(srcs, resolveLink);
            if (targetNode == null) {
                return null;
            } else {
                return createFileStatus(HdfsFileStatus.EMPTY_NAME, targetNode);
            }
        } finally {
            readUnlock();
        }
    }

    /**
     * Get the blocks associated with the file.
     *
     * @throws IOException
     */
    Block[] getFileBlocks(String src) throws IOException, PersistanceException {
        waitForReady();
        readLock();
        try {
            INode targetNode = getRootDir().getNode(src, false);
            if (targetNode == null) {
                return null;
            }
            if (targetNode.isDirectory()) {
                return null;
            }
            if (targetNode.isLink()) {
                return null;
            }
            return (Block[]) ((INodeFile) targetNode).getBlocks().toArray();
        } finally {
            readUnlock();
        }
    }

    /**
     * Validate if INode correctly points to a file
     */
    boolean isValidINodeFile(INode inode) {
        if (inode == null || inode.isDirectory()) {
            return false;
        }
        assert !inode.isLink();
        return true;
    }

    /**
     * Get {@link INode} associated with the file.
     */
    INodeFile getFileINode(String src) throws UnresolvedLinkException, PersistanceException, IOException {
        readLock();
        try {
            INode inode = getRootDir().getNode(src, true);
            if (inode == null || inode.isDirectory()) {
                return null;
            }
            assert !inode.isLink();
            return (INodeFile) inode;
        } finally {
            readUnlock();
        }
    }

    /**
     * Retrieve the existing INodes along the given path.
     *
     * @param path the path to explore
     * @return INodes array containing the existing INodes in the order they
     * appear when following the path from the root INode to the deepest INodes.
     * The array size will be the number of expected components in the path, and
     * non existing components will be filled with null
     *
     * @see INodeDirectory#getExistingPathINodes(byte[][], INode[])
     */
    INode[] getExistingPathINodes(String path)
            throws UnresolvedLinkException, PersistanceException, IOException {
        readLock();
        try {
            return getRootDir().getExistingPathINodes(path, true);
        } finally {
            readUnlock();
        }
    }

    /**
     * Get the parent node of path.
     *
     * @param path the path to explore
     * @return its parent node
     */
    INodeDirectory getParent(byte[][] path)
            throws FileNotFoundException, UnresolvedLinkException, PersistanceException, IOException {
        readLock();
        try {
            return getRootDir().getParent(path);
        } finally {
            readUnlock();
        }
    }

    /**
     * Check whether the filepath could be created
     */
    boolean isValidToCreate(String src) throws UnresolvedLinkException, 
            PersistanceException,
            IOException{
        String srcs = normalizePath(src);
        readLock();
        try {
            if (srcs.startsWith("/")
                    && !srcs.endsWith("/")
                    && getRootDir().getNode(srcs, false) == null) {
                return true;
            } else {
                return false;
            }
        } finally {
            readUnlock();
        }
    }

    /**
     * Check whether the path specifies a directory
     */
    boolean isDir(String src) throws UnresolvedLinkException, PersistanceException, IOException {
        src = normalizePath(src);
        readLock();
        try {
            INode node = getRootDir().getNode(src, false);
            return node != null && node.isDirectory();
        } finally {
            readUnlock();
        }
    }

    /**
     * Updates namespace and diskspace consumed for all directories until the
     * parent directory of file represented by path.
     *
     * @param path path for the file.
     * @param nsDelta the delta change of namespace
     * @param dsDelta the delta change of diskspace
     * @throws QuotaExceededException if the new count violates any quota limit
     * @throws FileNotFound if path does not exist.
     */
    void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
            throws QuotaExceededException,
            FileNotFoundException,
            UnresolvedLinkException,
            PersistanceException,
            IOException{
        writeLock();
        try {
            INode[] inodes = getRootDir().getExistingPathINodes(path, false);
            int len = inodes.length;
            if (inodes[len - 1] == null) {
                throw new FileNotFoundException(path
                        + " does not exist under rootDir.");
            }
            updateCount(inodes, len - 1, nsDelta, dsDelta, true);
        } finally {
            writeUnlock();
        }
    }

    /**
     * update count of each inode with quota
     *
     * @param inodes an array of inodes on a path
     * @param numOfINodes the number of inodes to update starting from index 0
     * @param nsDelta the delta change of namespace
     * @param dsDelta the delta change of diskspace
     * @param checkQuota if true then check if quota is exceeded
     * @throws QuotaExceededException if the new count violates any quota limit
     */
    private void updateCount(INode[] inodes, int numOfINodes,
            long nsDelta, long dsDelta, boolean checkQuota)
            throws QuotaExceededException, PersistanceException {
        if (quotaEnabled) {
            assert hasWriteLock();
            if (!ready) {
                //still initializing. do not check or update quotas.
                return;
            }
            if (numOfINodes > inodes.length) {
                numOfINodes = inodes.length;
            }
            if (checkQuota) {
                verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
            }

            for (int i = 0; i < numOfINodes; i++) {
                if (inodes[i] instanceof INodeDirectory) { // a directory
                    INodeDirectory node = (INodeDirectory) inodes[i];
                    node.updateNumItemsInTree(nsDelta, dsDelta);
                    EntityManager.update(node);
                }
            }
        }
    }

    /**
     * update quota of each inode and check to see if quota is exceeded. See
     * {@link #updateCount(INode[], int, long, long, boolean)}
     */
    private void updateCountNoQuotaCheck(INode[] inodes, int numOfINodes,
            long nsDelta, long dsDelta) throws PersistanceException {
        assert hasWriteLock();
        try {
            updateCount(inodes, numOfINodes, nsDelta, dsDelta, false);
        } catch (QuotaExceededException e) {
            NameNode.LOG.warn("FSDirectory.updateCountNoQuotaCheck - unexpected ", e);
        }
    }

    /**
     * updates quota without verification callers responsibility is to make sure
     * quota is not exceeded
     *
     * @param inodes
     * @param numOfINodes
     * @param nsDelta
     * @param dsDelta
     */
    void unprotectedUpdateCount(INode[] inodes, int numOfINodes,
            long nsDelta, long dsDelta) throws PersistanceException {
        assert hasWriteLock();
        for (int i = 0; i < numOfINodes; i++) {
            if (inodes[i] instanceof INodeDirectory) { // a directory with quota
                INodeDirectory node = (INodeDirectory) inodes[i];
                node.updateNumItemsInTree(nsDelta, dsDelta);
                EntityManager.update(node);
            }
        }
    }

    /**
     * Return the name of the path represented by inodes at [0, pos]
     */
    private static String getFullPathName(INode[] inodes, int pos) {
        StringBuilder fullPathName = new StringBuilder();
        if (inodes[0].isRoot()) {
            if (pos == 0) {
                return Path.SEPARATOR;
            }
        } else {
            fullPathName.append(inodes[0].getName());
        }

        for (int i = 1; i <= pos; i++) {
            fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getName());
        }
        return fullPathName.toString();
    }

    /**
     * Return the full path name of the specified inode
     */
    static String getFullPathName(INode inode) throws PersistanceException {
        if (inode.getName().equals(INodeDirectory.ROOT_NAME)) {
            return INodeDirectory.ROOT_NAME;
        }
        return getFullPathName(EntityManager.find(INode.Finder.ByPKey, inode.getParentId())) + "/" + inode.getName();
    }

    @Deprecated
    static String getFullPathNameOld(INode inode) {

        // calculate the depth of this inode from root
        int depth = 0;
        for (INode i = inode; i != null; i = i.parent) {
            depth++;
        }
        INode[] inodes = new INode[depth];

        // fill up the inodes in the path from this inode to root
        for (int i = 0; i < depth; i++) {
            inodes[depth - i - 1] = inode;
            inode = inode.parent;
        }
        return getFullPathName(inodes, depth - 1);
    }

    /**
     * Create a directory If ancestor directories do not exist, automatically
     * create them.
     *
     * @param src string representation of the path to the directory
     * @param permissions the permission of the directory
     * @param inheritPermission if the permission of the directory should
     * inherit from its parent or not. The automatically created ones always
     * inherit its permission from its parent
     * @param now creation time
     * @return true if the operation succeeds false otherwise
     * @throws FileNotFoundException if an ancestor or itself is a file
     * @throws QuotaExceededException if directory creation violates any quota
     * limit
     * @throws UnresolvedLinkException if a symlink is encountered in src.
     */
    boolean mkdirs(String src, PermissionStatus permissions,
            boolean inheritPermission, long now)
            throws FileAlreadyExistsException, QuotaExceededException,
            UnresolvedLinkException, PersistanceException,
            IOException{
        src = normalizePath(src);
        String[] names = INode.getPathNames(src);
        byte[][] components = INode.getPathComponents(names);
        INode[] inodes = new INode[components.length];

        writeLock();
        try {
            getRootDir().getExistingPathINodes(components, inodes, false);

            // find the index of the first null in inodes[]
            StringBuilder pathbuilder = new StringBuilder();
            int i = 1;
            for (; i < inodes.length && inodes[i] != null; i++) {
                pathbuilder.append(Path.SEPARATOR + names[i]);
                if (!inodes[i].isDirectory()) {
                    throw new FileAlreadyExistsException("Parent path is not a directory: "
                            + pathbuilder + " " + inodes[i].getName());
                }
            }

            // create directories beginning from the first null index
            for (; i < inodes.length; i++) {
                pathbuilder.append(Path.SEPARATOR + names[i]);
                String cur = pathbuilder.toString();
                unprotectedMkdir(inodes, i, components[i], permissions,
                        inheritPermission || i != components.length - 1, now);
                if (inodes[i] == null) {
                    return false;
                }
                // Directory creation also count towards FilesCreated
                // to match count of FilesDeleted metric.
                if (getFSNamesystem() != null) {
                    NameNode.getNameNodeMetrics().incrFilesCreated();
                }
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug(
                            "DIR* FSDirectory.mkdirs: created directory " + cur);
                }
            }
        } finally {
            writeUnlock();
        }
        return true;
    }

    /**
     * //TODO: kamal, TX
     */
    INode unprotectedMkdir(String src, PermissionStatus permissions,
            long timestamp) throws QuotaExceededException,
            UnresolvedLinkException,
            PersistanceException,
            IOException{
        assert hasWriteLock();
        byte[][] components = INode.getPathComponents(src);
        INode[] inodes = new INode[components.length];

        getRootDir().getExistingPathINodes(components, inodes, false);
        unprotectedMkdir(inodes, inodes.length - 1, components[inodes.length - 1],
                permissions, false, timestamp);
        return inodes[inodes.length - 1];
    }

    /**
     * create a directory at index pos. The parent path to the directory is at
     * [0, pos-1]. All ancestors exist. Newly created one stored at index pos.
     */
    private void unprotectedMkdir(INode[] inodes, int pos,
            byte[] name, PermissionStatus permission, boolean inheritPermission,
            long timestamp) throws QuotaExceededException, PersistanceException {
        assert hasWriteLock();
        inodes[pos] = addChild(inodes, pos,
                new INodeDirectory(name, permission, timestamp),
                -1, inheritPermission);
    }

    /**
     * Add a node child to the namespace. The full path name of the node is src.
     * childDiskspace should be -1, if unknown. QuotaExceededException is thrown
     * if it violates quota limit
     */
    private <T extends INode> T addNode(String src, T child,
            long childDiskspace, boolean inheritPermission)
            throws QuotaExceededException, UnresolvedLinkException, 
            PersistanceException, IOException {
      return addNode(src, child, childDiskspace, inheritPermission, false);
    }
    /**
     * Add a node child to the namespace. The full path name of the node is src.
     * childDiskspace should be -1, if unknown. QuotaExceededException is thrown
     * if it violates quota limit
     */
    private <T extends INode> T addNode(String src, T child,
            long childDiskspace, boolean inheritPermission, boolean reuseId)
            throws QuotaExceededException, UnresolvedLinkException, 
            PersistanceException, IOException {

        byte[][] components = INode.getPathComponents(src);
        byte[] path = components[components.length - 1];
        child.setName(path);
        cacheName(child);
        INode[] inodes = new INode[components.length];
        writeLock();
        try {
            getRootDir().getExistingPathINodes(components, inodes, false);
            return addChild(inodes, inodes.length - 1, child, childDiskspace,
                    inheritPermission, reuseId);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Verify quota for adding or moving a new INode with required namespace and
     * diskspace to a given position.
     *
     * @param inodes INodes corresponding to a path
     * @param pos position where a new INode will be added
     * @param nsDelta needed namespace
     * @param dsDelta needed diskspace
     * @param commonAncestor Last node in inodes array that is a common ancestor
     * for a INode that is being moved from one location to the other. Pass null
     * if a node is not being moved.
     * @throws QuotaExceededException if quota limit is exceeded.
     */
    private void verifyQuota(INode[] inodes, int pos, long nsDelta, long dsDelta,
            INode commonAncestor) throws QuotaExceededException, PersistanceException {
        if (!ready) {
            // Do not check quota if edits log is still being processed
            return;
        }
        if (nsDelta <= 0 && dsDelta <= 0) {
            // if quota is being freed or not being consumed
            return;
        }
        if (pos > inodes.length) {
            pos = inodes.length;
        }
        int i = pos - 1;
        try {
            // check existing components in the path  
            for (; i >= 0; i--) {
                if (commonAncestor != null && commonAncestor.equals(inodes[i])) {
                    // Moving an existing node. Stop checking for quota when common
                    // ancestor is reached
                    return;
                }
                if (inodes[i].isQuotaSet()) { // a directory with quota
                    INodeDirectoryWithQuota node = (INodeDirectoryWithQuota) inodes[i];
                    node.verifyQuota(nsDelta, dsDelta);
                }
            }
        } catch (QuotaExceededException e) {
            e.setPathName(inodes[i].getFullPathName());
            throw e;
        }
    }

    /**
     * Verify quota for rename operation where srcInodes[srcInodes.length-1]
     * moves dstInodes[dstInodes.length-1]
     *
     * @param srcInodes directory from where node is being moved.
     * @param dstInodes directory to where node is moved to.
     * @throws QuotaExceededException if quota limit is exceeded.
     */
    private void verifyQuotaForRename(INode[] srcInodes, INode[] dstInodes)
            throws QuotaExceededException, PersistanceException {
        if (!ready) {
            // Do not check quota if edits log is still being processed
            return;
        }
        INode srcInode = srcInodes[srcInodes.length - 1];
        INode commonAncestor = null;
        for (int i = 0; srcInodes[i].equals(dstInodes[i]); i++) {
            commonAncestor = srcInodes[i];
        }
        INode.DirCounts srcCounts = new INode.DirCounts();
        srcInode.spaceConsumedInTree(srcCounts);
        long nsDelta = srcCounts.getNsCount();
        long dsDelta = srcCounts.getDsCount();

        // Reduce the required quota by dst that is being removed
        INode dstInode = dstInodes[dstInodes.length - 1];
        if (dstInode != null) {
            INode.DirCounts dstCounts = new INode.DirCounts();
            dstInode.spaceConsumedInTree(dstCounts);
            nsDelta -= dstCounts.getNsCount();
            dsDelta -= dstCounts.getDsCount();
        }
        verifyQuota(dstInodes, dstInodes.length - 1, nsDelta, dsDelta,
                commonAncestor);
    }

    /**
     * Verify that filesystem limit constraints are not violated
     *
     * @throws PathComponentTooLongException child's name is too long
     * @throws MaxDirectoryItemsExceededException items per directory is
     * exceeded
     */
    protected <T extends INode> void verifyFsLimits(INode[] pathComponents,
            int pos, T child) throws FSLimitException, PersistanceException {
        boolean includeChildName = false;
        try {
            if (maxComponentLength != 0) {
                int length = child.getName().length();
                if (length > maxComponentLength) {
                    includeChildName = true;
                    throw new PathComponentTooLongException(maxComponentLength, length);
                }
            }
            if (maxDirItems != 0) {
                INodeDirectory parent = (INodeDirectory) pathComponents[pos - 1];
                int count = parent.getChildren().size();
                if (count >= maxDirItems) {
                    throw new MaxDirectoryItemsExceededException(maxDirItems, count);
                }
            }
        } catch (FSLimitException e) {
            String badPath = pathComponents[pos - 1].getFullPathName();//getFullPathName(pathComponents, pos-1);
            if (includeChildName) {
                badPath += Path.SEPARATOR + child.getName();
            }
            e.setPathName(badPath);
            // Do not throw if edits log is still being processed
            if (ready) {
                throw (e);
            }
            // log pre-existing paths that exceed limits
            NameNode.LOG.error("FSDirectory.verifyFsLimits - " + e.getLocalizedMessage());
        }
    }

    /**
     * Add a node child to the inodes at index pos. Its ancestors are stored at
     * [0, pos-1]. QuotaExceededException is thrown if it violates quota limit
     */
    private <T extends INode> T addChild(INode[] pathComponents, int pos,
            T child, long childDiskspace, boolean inheritPermission,
            boolean checkQuota, boolean reuseID) throws QuotaExceededException, PersistanceException {
        // The filesystem limits are not really quotas, so this check may appear
        // odd.  It's because a rename operation deletes the src, tries to add
        // to the dest, if that fails, re-adds the src from whence it came.
        // The rename code disables the quota when it's restoring to the
        // original location becase a quota violation would cause the the item
        // to go "poof".  The fs limits must be bypassed for the same reason.
        if (checkQuota) {
            verifyFsLimits(pathComponents, pos, child);
        }

        INode.DirCounts counts = new INode.DirCounts();
        child.spaceConsumedInTree(counts);
        if (childDiskspace < 0) {
            childDiskspace = counts.getDsCount();
        }
        updateCount(pathComponents, pos, counts.getNsCount(), childDiskspace,
                checkQuota);
        if (pathComponents[pos - 1] == null) {
            throw new NullPointerException("Panic: parent does not exist");
        }
        T addedNode = ((INodeDirectory) pathComponents[pos - 1]).addChild(
                child, inheritPermission, true, reuseID);
        if (addedNode == null) {
            updateCount(pathComponents, pos, -counts.getNsCount(),
                    -childDiskspace, true);
        } else {
//            if (reuseID) {
//                EntityManager.update(addedNode); //for move or rename
//            } else {
                EntityManager.add(addedNode);
//            }
            // [H] The following updates modification time
            if (quotaEnabled) {
                EntityManager.update(pathComponents[pos - 1]);
            }
        }

        return addedNode;
    }
    
    private <T extends INode> T addChild(INode[] pathComponents, int pos,
            T child, long childDiskspace, boolean inheritPermission, boolean reuseId)
            throws QuotaExceededException, PersistanceException {
      return addChild(pathComponents, pos, child, childDiskspace, inheritPermission, true, reuseId);
    }

    private <T extends INode> T addChild(INode[] pathComponents, int pos,
            T child, long childDiskspace, boolean inheritPermission)
            throws QuotaExceededException, PersistanceException {

        return addChild(pathComponents, pos, child, childDiskspace,
                inheritPermission, true, false);
    }

    private <T extends INode> T addChildNoQuotaCheck(INode[] pathComponents,
            int pos, T child, long childDiskspace, boolean inheritPermission, boolean reuseID) throws PersistanceException {
        T inode = null;
        try {
            inode = addChild(pathComponents, pos, child, childDiskspace,
                    inheritPermission, false, reuseID);
        } catch (QuotaExceededException e) {
            NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
        }
        return inode;
    }

    /**
     * Remove an inode at index pos from the namespace. Its ancestors are stored
     * at [0, pos-1]. Count of each ancestor with quota is also updated. Return
     * the removed node; null if the removal fails.
     */
    private INode removeChild(INode[] pathComponents, int pos) throws PersistanceException {
        INodeDirectory dir = ((INodeDirectory) pathComponents[pos - 1]);	//parent of file/dir being removed
        INode removedNode = dir.removeChild(pathComponents[pos]);
        INode.DirCounts counts = new INode.DirCounts();
        removedNode.spaceConsumedInTree(counts);
        updateCountNoQuotaCheck(pathComponents, pos,
                -counts.getNsCount(), -counts.getDsCount());
        EntityManager.remove(removedNode);
        return removedNode;
    }

    /**
     */
    String normalizePath(String src) {
        if (src.length() > 1 && src.endsWith("/")) {
            src = src.substring(0, src.length() - 1);
        }
        return src;
    }

    ContentSummary getContentSummary(String src)
            throws FileNotFoundException, UnresolvedLinkException,
            PersistanceException, IOException {
        String srcs = normalizePath(src);
        readLock();
        try {
            INode targetNode = getRootDir().getNode(srcs, false);
            if (targetNode == null) {
                throw new FileNotFoundException("File does not exist: " + srcs);
            } else {
                return targetNode.computeContentSummary();
            }
        } finally {
            readUnlock();
        }
    }

    /**
     * Update the count of each directory with quota in the namespace A
     * directory's count is defined as the total number inodes in the tree
     * rooted at the directory.
     *
     * This is an update of existing state of the filesystem and does not throw
     * QuotaExceededException.
     */
    void updateCountForINodeDirecotry() throws PersistanceException, IOException {
        updateCountForINodeDirectory(getRootDir(), new INode.DirCounts(),
                new ArrayList<INode>(50));
    }

    /**
     * Update the count of the directory if it has a quota and return the count
     *
     * This does not throw a QuotaExceededException. This is just an update of
     * of existing state and throwing QuotaExceededException does not help with
     * fixing the state, if there is a problem.
     *
     * @param dir the root of the tree that represents the directory
     * @param counters counters for name space and disk space
     * @param nodesInPath INodes for the each of components in the path.
     */
    private static void updateCountForINodeDirectory(INodeDirectory dir,
            INode.DirCounts counts,
            ArrayList<INode> nodesInPath) throws PersistanceException {
        long parentNamespace = counts.nsCount;
        long parentDiskspace = counts.dsCount;

        counts.nsCount = 1L;//for self. should not call node.spaceConsumedInTree()
        counts.dsCount = 0L;

        /*
         * We don't need nodesInPath if we could use 'parent' field in INode. using
         * 'parent' is not currently recommended.
         */
        nodesInPath.add(dir);

        for (INode child : dir.getChildren()) {
            if (child.isDirectory()) {
                updateCountForINodeDirectory((INodeDirectory) child,
                        counts, nodesInPath);
            } else if (child.isLink()) {
                counts.nsCount += 1;
            } else { // reduce recursive calls
                counts.nsCount += 1;
                counts.dsCount += ((INodeFile) child).diskspaceConsumed();
            }
        }

        if (dir instanceof INodeDirectory) {
            ((INodeDirectory) dir).setSpaceConsumed(counts.nsCount,
                    counts.dsCount);

            // check if quota is violated for some reason.
            if ((dir.getNsQuota() >= 0 && counts.nsCount > dir.getNsQuota())
                    || (dir.getDsQuota() >= 0 && counts.dsCount > dir.getDsQuota())) {

                // can only happen because of a software bug. the bug should be fixed.
                StringBuilder path = new StringBuilder(512);
                for (INode n : nodesInPath) {
                    path.append('/');
                    path.append(n.getName());
                }

                NameNode.LOG.warn("Quota violation in image for " + path
                        + " (Namespace quota : " + dir.getNsQuota()
                        + " consumed : " + counts.nsCount + ")"
                        + " (Diskspace quota : " + dir.getDsQuota()
                        + " consumed : " + counts.dsCount + ").");
            }
        }

        // pop 
        nodesInPath.remove(nodesInPath.size() - 1);

        counts.nsCount += parentNamespace;
        counts.dsCount += parentDiskspace;
    }

    /**
     * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
     * Sets quota for for a directory.
     *
     * @returns INodeDirectory if any of the quotas have changed. null other
     * wise.
     *
     * @throws FileNotFoundException if the path does not exist or is a file
     * @throws QuotaExceededException if the directory tree size is greater than
     * the given quota
     * @throws UnresolvedLinkException if a symlink is encountered in src.
     */
    INodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota)
            throws FileNotFoundException, QuotaExceededException,
            UnresolvedLinkException, PersistanceException, IOException {
        assert hasWriteLock();
        // sanity check
        if ((nsQuota < 0 && nsQuota != HdfsConstants.QUOTA_DONT_SET
                && nsQuota < HdfsConstants.QUOTA_RESET)
                || (dsQuota < 0 && dsQuota != HdfsConstants.QUOTA_DONT_SET
                && dsQuota < HdfsConstants.QUOTA_RESET)) {
            throw new IllegalArgumentException("Illegal value for nsQuota or "
                    + "dsQuota : " + nsQuota + " and "
                    + dsQuota);
        }

        String srcs = normalizePath(src);

        INode[] inodes = getRootDir().getExistingPathINodes(src, true);
        INode targetNode = inodes[inodes.length - 1];
        if (targetNode == null) {
            throw new FileNotFoundException("Directory does not exist: " + srcs);
        } else if (!targetNode.isDirectory()) {
            throw new FileNotFoundException("Cannot set quota on a file: " + srcs);
        } else if (targetNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
            throw new IllegalArgumentException("Cannot clear namespace quota on root.");
        } else { // a directory inode
            INodeDirectory dirNode = (INodeDirectory) targetNode;
            long oldNsQuota = dirNode.getNsQuota();
            long oldDsQuota = dirNode.getDsQuota();
            if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
                nsQuota = oldNsQuota;
            }
            if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
                dsQuota = oldDsQuota;
            }

            if (dirNode instanceof INodeDirectoryWithQuota) {
                // a directory with quota; so set the quota to the new value
                ((INodeDirectoryWithQuota) dirNode).setQuota(nsQuota, dsQuota);
                EntityManager.update(dirNode);
            } else {
                // a non-quota directory; so replace it with a directory with quota
                INodeDirectoryWithQuota newNode =
                        new INodeDirectoryWithQuota(nsQuota, dsQuota, dirNode);
                // non-root directory node; parent != null
                INodeDirectory parent = (INodeDirectory) inodes[inodes.length - 2];
                parent.replaceChild(dirNode, newNode);
                EntityManager.update(newNode);
                dirNode = newNode;
            }
            return (oldNsQuota != nsQuota || oldDsQuota != dsQuota) ? dirNode : null;
        }
    }

    /**
     * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
     *
     * @see #unprotectedSetQuota(String, long, long)
     */
    void setQuota(String src, long nsQuota, long dsQuota)
            throws FileNotFoundException, QuotaExceededException,
            UnresolvedLinkException, PersistanceException, 
            IOException{
        writeLock();
        try {
            unprotectedSetQuota(src, nsQuota, dsQuota);
        } finally {
            writeUnlock();
        }
    }

  int totalInodes() throws IOException {
    readLock();
    try {
      // TODO[Hooman]: after fixing quota, we can use root.getNscount instead of this.
      LightWeightRequestHandler totalInodesHandler = new LightWeightRequestHandler(RequestHandler.OperationType.TOTAL_FILES) {
        @Override
        public Object performTask() throws PersistanceException, IOException {
          InodeDataAccess da = (InodeDataAccess) StorageFactory.getDataAccess(InodeDataAccess.class);
          return da.countAll();
        }
      };
      return (Integer) totalInodesHandler.handle();
    } finally {
      readUnlock();
    }
  }

    /**
     * Sets the access time on the file. Logs it in the transaction log.
     */
    void setTimes(String src, INodeFile inode, long mtime, long atime, boolean force) throws PersistanceException {

        unprotectedSetTimes(src, inode, mtime, atime, force);
    }

    //unused
    boolean unprotectedSetTimes(String src, long mtime, long atime, boolean force)
            throws UnresolvedLinkException, PersistanceException, IOException {
        //assert hasWriteLock();
        INodeFile inode = getFileINode(src);
        return unprotectedSetTimes(src, inode, mtime, atime, force);
    }

    private boolean unprotectedSetTimes(String src, INodeFile inode, long mtime,
            long atime, boolean force) throws PersistanceException {
        //assert hasWriteLock();
        boolean status = false;
        if (mtime != -1) {
            inode.setModificationTimeForce(mtime);
            EntityManager.update(inode);
            status = true;
        }
        if (atime != -1) {
            long inodeTime = inode.getAccessTime();

            // if the last access time update was within the last precision interval, then
            // no need to store access time
            if (atime <= inodeTime + getFSNamesystem().getAccessTimePrecision() && !force) {
                status = false;
            } else {
                inode.setAccessTime(atime);
                EntityManager.update(inode);
                status = true;
            }
        }
        return status;
    }

    /**
     * Reset the entire namespace tree.
     */
    void reset() throws PersistanceException{
        INode rootDir = new INodeDirectoryWithQuota(INodeDirectory.ROOT_NAME,
                getFSNamesystem().createFsOwnerPermissions(new FsPermission((short) 0755)),
                Integer.MAX_VALUE, -1);
        EntityManager.update(rootDir);
    }

    /**
     * create an hdfs file status from an inode
     *
     * @param path the local name
     * @param node inode
     * @param needLocation if block locations need to be included or not
     * @return a file status
     * @throws IOException if any error occurs
     */
    private HdfsFileStatus createFileStatus(byte[] path, INode node,
            boolean needLocation) throws IOException, PersistanceException {
        if (needLocation) {
            return createLocatedFileStatus(path, node);
        } else {
            return createFileStatus(path, node);
        }
    }

    /**
     * Create FileStatus by file INode
     */
    private HdfsFileStatus createFileStatus(byte[] path, INode node) throws PersistanceException {
        long size = 0;     // length is zero for directories
        short replication = 0;
        long blocksize = 0;
        if (node instanceof INodeFile) {
            INodeFile fileNode = (INodeFile) node;
            size = fileNode.computeFileSize(true);
            replication = fileNode.getReplication();
            blocksize = fileNode.getPreferredBlockSize();
        }
        return new HdfsFileStatus(
                size,
                node.isDirectory(),
                replication,
                blocksize,
                node.getModificationTime(),
                node.getAccessTime(),
                node.getFsPermission(),
                node.getUserName(),
                node.getGroupName(),
                node.isLink() ? ((INodeSymlink) node).getSymlink() : null,
                path);
    }

    /**
     * Create FileStatus with location info by file INode
     */
    private HdfsLocatedFileStatus createLocatedFileStatus(
            byte[] path, INode node) throws IOException, PersistanceException {
        assert hasReadLock();
        long size = 0;     // length is zero for directories
        short replication = 0;
        long blocksize = 0;
        LocatedBlocks loc = null;
        if (node instanceof INodeFile) {
            INodeFile fileNode = (INodeFile) node;
            size = fileNode.computeFileSize(true);
            replication = fileNode.getReplication();
            blocksize = fileNode.getPreferredBlockSize();
            loc = getFSNamesystem().getBlockManager().createLocatedBlocks(
                    fileNode.getBlocks(), fileNode.computeFileSize(false),
                    fileNode.isUnderConstruction(), 0L, size, false);
            if (loc == null) {
                loc = new LocatedBlocks();
            }
        }
        return new HdfsLocatedFileStatus(
                size,
                node.isDirectory(),
                replication,
                blocksize,
                node.getModificationTime(),
                node.getAccessTime(),
                node.getFsPermission(),
                node.getUserName(),
                node.getGroupName(),
                node.isLink() ? ((INodeSymlink) node).getSymlink() : null,
                path,
                loc);
    }

    /**
     * Add the given symbolic link to the fs. Record it in the edits log.
     */
    INodeSymlink addSymlink(String path, String target,
            PermissionStatus dirPerms, boolean createParent)
            throws UnresolvedLinkException, FileAlreadyExistsException,
            QuotaExceededException, IOException, PersistanceException {
        waitForReady();

        final long modTime = now();
        if (createParent) {
            final String parent = new Path(path).getParent().toString();
            if (!mkdirs(parent, dirPerms, true, modTime)) {
                return null;
            }
        }
        final String userName = dirPerms.getUserName();
        INodeSymlink newNode = null;
        writeLock();
        try {
            newNode = unprotectedSymlink(path, target, modTime, modTime,
                    new PermissionStatus(userName, null, FsPermission.getDefault()));
        } finally {
            writeUnlock();
        }
        if (newNode == null) {
            NameNode.stateChangeLog.info("DIR* FSDirectory.addSymlink: "
                    + "failed to add " + path
                    + " to the file system");
            return null;
        }

        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.addSymlink: "
                    + path + " is added to the file system");
        }
        return newNode;
    }

    /**
     * Add the specified path into the namespace. Invoked from edit log
     * processing.
     */
    INodeSymlink unprotectedSymlink(String path, String target, long modTime,
            long atime, PermissionStatus perm)
            throws UnresolvedLinkException, PersistanceException {
        assert hasWriteLock();
        INodeSymlink newNode = new INodeSymlink(target, modTime, atime, perm);
        try {
            newNode = addNode(path, newNode, UNKNOWN_DISK_SPACE, false);
        } catch (UnresolvedLinkException e) {
            /*
             * All UnresolvedLinkExceptions should have been resolved by now, but we
             * should re-throw them in case that changes so they are not swallowed by
             * catching IOException below.
             */
            throw e;
        } catch (IOException e) {
            return null;
        }
        return newNode;
    }

    /**
     * Caches frequently used file names to reuse file name objects and reduce
     * heap size.
     */
    void cacheName(INode inode) {
        // Name is cached only for files
        if (inode.isDirectory() || inode.isLink()) {
            return;
        }
        ByteArray name = new ByteArray(inode.getNameBytes());
        name = nameCache.put(name);
        if (name != null) {
            inode.setName(name.getBytes());
        }
    }

  /**
   * Created for acquiring locks in KTHFS
   *
   * @return
   */
  public INodeDirectoryWithQuota getRootDir() throws PersistanceException, IOException {
    return (INodeDirectoryWithQuota) EntityManager.find(INode.Finder.ByNameAndParentId, ROOT, ROOT_PARENT_ID);
  }

  public boolean isQuotaEnabled() {
    return this.quotaEnabled;
  }
}
