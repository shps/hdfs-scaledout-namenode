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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportIterator;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManagerNN;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager.LockType;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.CorruptReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ExcessReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster. This
 * class is a helper class for {@link FSNamesystem} and requires several methods
 * to be called with lock held on {@link FSNamesystem}.
 */
@InterfaceAudience.Private
public class BlockManager {

    static final Log LOG = LogFactory.getLog(BlockManager.class);
    /**
     * Default load factor of map
     */
    public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;
    public static final int UNDER_REPLICATED_LEVEL_FOR_CORRUPTS = 4;
    private final Namesystem namesystem;
    private DatanodeManager datanodeManager = null;
    private HeartbeatManager heartbeatManager = null;
    private BlockTokenSecretManagerNN blockTokenSecretManager;
    private volatile long pendingReplicationBlocksCount = 0L;
    private volatile long corruptReplicaBlocksCount = 0L;
    private volatile long underReplicatedBlocksCount = 0L;
    private volatile long scheduledReplicationBlocksCount = 0L;

    /**
     * Used by metrics
     */
    public long getPendingReplicationBlocksCount() {
        return pendingReplicationBlocksCount;
    }

    /**
     * Used by metrics
     */
    public long getUnderReplicatedBlocksCount() {
        return underReplicatedBlocksCount;
    }

    /**
     * Used by metrics
     */
    public long getCorruptReplicaBlocksCount() {
        return corruptReplicaBlocksCount;
    }

    /**
     * Used by metrics
     */
    public long getScheduledReplicationBlocksCount() {
        return scheduledReplicationBlocksCount;
    }

    /**
     * Used by metrics
     */
    public long getPendingDeletionBlocksCount(OperationType opType) throws IOException {
        return invalidateBlocks.numBlocks(opType);
    }

    /**
     * Used by metrics
     */
    public long getExcessBlocksCount(OperationType opType) throws IOException {
        return (Integer) new LightWeightRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                ExcessReplicaDataAccess da = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
                return da.countAll();
            }
        }.handle();
    }
    /**
     * replicationRecheckInterval is how often namenode checks for new
     * replication work
     */
    private final long replicationRecheckInterval;
    /**
     * Replication thread.
     */
    Daemon replicationThread;
    /**
     * Blocks to be invalidated.
     */
    private InvalidateBlocks invalidateBlocks = null;
    //
    // Store set of Blocks that need to be replicated 1 or more times.
    // We also store pending replication-orders.
    //
    public UnderReplicatedBlocks neededReplications = null;
    @VisibleForTesting
    PendingReplicationBlocks pendingReplications = null;
    /**
     * The maximum number of replicas allowed for a block
     */
    public final short maxReplication;
    /**
     * The maximum number of outgoing replication streams a given node should
     * have at one time
     */
    int maxReplicationStreams;
    /**
     * Minimum copies needed or else write is disallowed
     */
    public final short minReplication;
    /**
     * Default number of replicas
     */
    public final int defaultReplication;
    /**
     * The maximum number of entries returned by getCorruptInodes()
     */
    final int maxCorruptFilesReturned;
    /**
     * variable to enable check for enough racks
     */
    final boolean shouldCheckForEnoughRacks;
    /**
     * Last block index used for replication work.
     */
    private int replIndex = 0;
    /**
     * for block replicas placement
     */
    private BlockPlacementPolicy blockplacement;

    public BlockManager(FSNamesystem fsn, Configuration conf) throws IOException {
        namesystem = fsn;

        datanodeManager = new DatanodeManager(this, fsn, conf);
        heartbeatManager = datanodeManager.getHeartbeatManager();
        invalidateBlocks = new InvalidateBlocks(datanodeManager);
        neededReplications = new UnderReplicatedBlocks();
        blockplacement = BlockPlacementPolicy.getInstance(
                conf, fsn, datanodeManager.getNetworkTopology());
        pendingReplications = new PendingReplicationBlocks(conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT) * 1000L);
        replicationThread = new Daemon(new ReplicationMonitor());

        this.maxCorruptFilesReturned = conf.getInt(
                DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY,
                DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED);
        this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
                DFSConfigKeys.DFS_REPLICATION_DEFAULT);

        final int maxR = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY,
                DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
        final int minR = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
        if (minR <= 0) {
            throw new IOException("Unexpected configuration parameters: "
                    + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
                    + " = " + minR + " <= 0");
        }
        if (maxR > Short.MAX_VALUE) {
            throw new IOException("Unexpected configuration parameters: "
                    + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
                    + " = " + maxR + " > " + Short.MAX_VALUE);
        }
        if (minR > maxR) {
            throw new IOException("Unexpected configuration parameters: "
                    + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
                    + " = " + minR + " > "
                    + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
                    + " = " + maxR);
        }
        this.minReplication = (short) minR;
        this.maxReplication = (short) maxR;

        this.maxReplicationStreams = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT);
        this.shouldCheckForEnoughRacks = conf.get(DFSConfigKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY) == null ? false
                : true;

        this.replicationRecheckInterval =
                conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000L;
        LOG.info("defaultReplication = " + defaultReplication);
        LOG.info("maxReplication     = " + maxReplication);
        LOG.info("minReplication     = " + minReplication);
        LOG.info("maxReplicationStreams      = " + maxReplicationStreams);
        LOG.info("shouldCheckForEnoughRacks  = " + shouldCheckForEnoughRacks);
        LOG.info("replicationRecheckInterval = " + replicationRecheckInterval);
    }

    private static BlockTokenSecretManagerNN createBlockTokenSecretManager(
            FSNamesystem fsns,
            final Configuration conf) throws IOException {
        final boolean isEnabled = conf.getBoolean(
                DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
                DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
        LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + "=" + isEnabled);

        if (!isEnabled) {
            return null;
        }

        final long updateMin = conf.getLong(
                DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY,
                DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT);
        final long lifetimeMin = conf.getLong(
                DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY,
                DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT);
        LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY
                + "=" + updateMin + " min(s), "
                + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY
                + "=" + lifetimeMin + " min(s)");
        return new BlockTokenSecretManagerNN(true,
                updateMin * 60 * 1000L, lifetimeMin * 60 * 1000L,
                fsns.isLeader());
    }

    /**
     * get the BlockTokenSecretManager
     */
    BlockTokenSecretManagerNN getBlockTokenSecretManager() {
        return blockTokenSecretManager;
    }

    private boolean isBlockTokenEnabled() {
        return blockTokenSecretManager != null;
    }

    /**
     * Should the access keys be updated?
     */
    boolean shouldUpdateBlockKey(boolean isLeader, final long updateTime) throws IOException {
        return isBlockTokenEnabled() ? blockTokenSecretManager.updateKeys(isLeader, updateTime)
                : false;
    }

    public void activate(Configuration conf) throws IOException {
        datanodeManager.activate(conf);
        replicationThread.start();
      blockTokenSecretManager = createBlockTokenSecretManager((FSNamesystem)namesystem, conf);
    }

    public void close() {
        if (replicationThread != null) {
            replicationThread.interrupt();

        }
        if (datanodeManager != null) {
            datanodeManager.close();
        }
    }

    /**
     * @return the datanodeManager
     */
    public DatanodeManager getDatanodeManager() {
        return datanodeManager;
    }

    /**
     * @return the BlockPlacementPolicy
     */
    public BlockPlacementPolicy getBlockPlacementPolicy() {
        return blockplacement;
    }

    /**
     * Set BlockPlacementPolicy
     */
    public void setBlockPlacementPolicy(BlockPlacementPolicy newpolicy) {
        if (newpolicy == null) {
            throw new HadoopIllegalArgumentException("newpolicy == null");
        }
        this.blockplacement = newpolicy;
    }

    /**
     * Dump meta data to out.
     *
     * @throws IOException
     */
    public void metaSave(PrintWriter out) throws IOException, PersistanceException {
        assert namesystem.hasWriteLock();
        //
        // Dump contents of neededReplication
        //
        synchronized (neededReplications) {
            // TODO HOOMAN[lock] 
//      out.println("Metasave: Blocks waiting for replication: "
//              + neededReplications.size());
            for (UnderReplicatedBlock urb : EntityManager.findList(UnderReplicatedBlock.Finder.All)) {
                Block block = EntityManager.find(BlockInfo.Finder.ById, urb.getBlockId());
                List<DatanodeDescriptor> containingNodes =
                        new ArrayList<DatanodeDescriptor>();
                List<DatanodeDescriptor> containingLiveReplicasNodes =
                        new ArrayList<DatanodeDescriptor>();

                NumberReplicas numReplicas = new NumberReplicas();
                // source node returned is not used
                chooseSourceDatanode(block, containingNodes,
                        containingLiveReplicasNodes, numReplicas);
                assert containingLiveReplicasNodes.size() == numReplicas.liveReplicas();
                int usableReplicas = numReplicas.liveReplicas()
                        + numReplicas.decommissionedReplicas();

                if (block instanceof BlockInfo) {
                    String fileName = ((BlockInfo) block).getINode().getFullPathName();
                    out.print(fileName + ": ");
                }
                // l: == live:, d: == decommissioned c: == corrupt e: == excess
                out.print(block + ((usableReplicas > 0) ? "" : " MISSING")
                        + " (replicas:"
                        + " l: " + numReplicas.liveReplicas()
                        + " d: " + numReplicas.decommissionedReplicas()
                        + " c: " + numReplicas.corruptReplicas()
                        + " e: " + numReplicas.excessReplicas() + ") ");
                List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(block));

                for (DatanodeDescriptor node : dataNodes) {
                    String state = "";
                    if (isItCorruptedReplica(block.getBlockId(), node.getStorageID())) {
                        state = "(corrupt)";
                    } else if (node.isDecommissioned()
                            || node.isDecommissionInProgress()) {
                        state = "(decommissioned)";
                    }
                    out.print(" " + node + state + " : ");
                }

                out.println("");
            }
        }

        // Dump blocks from pendingReplication
        pendingReplications.metaSave(out);

        // Dump blocks that are waiting to be deleted
        invalidateBlocks.dump(out);

        // Dump all datanodes
        getDatanodeManager().datanodeDump(out);
    }

    /**
     * @return maxReplicationStreams
     */
    public int getMaxReplicationStreams() {
        return maxReplicationStreams;
    }

    /**
     * @param block
     * @return true if the block has minimum replicas
     */
    public boolean checkMinReplication(Block block) throws IOException, PersistanceException {
        return (countNodes(block).liveReplicas() >= minReplication);
    }

    /**
     * Commit a block of a file
     *
     * @param block block to be committed
     * @param commitBlock - contains client reported block length and generation
     * @return true if the block is changed to committed state.
     * @throws IOException if the block does not have at least a minimal number
     * of replicas reported from data-nodes.
     */
    private boolean commitBlock(final BlockInfoUnderConstruction block,
            final Block commitBlock) throws IOException, PersistanceException {
        if (block.getBlockUCState() == BlockUCState.COMMITTED) {
            return false;
        }
        assert block.getNumBytes() <= commitBlock.getNumBytes() :
                "commitBlock length is less than the stored one "
                + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
        block.commitBlock(commitBlock.getNumBytes(), commitBlock.getGenerationStamp());
        EntityManager.update(block);
        return true;
    }

    /**
     * Commit the last block of the file and mark it as complete if it has meets
     * the minimum replication requirement
     *
     * @param fileINode file inode
     * @param commitBlock - contains client reported block length and generation
     * @return true if the last block is changed to committed state.
     * @throws IOException if the block does not have at least a minimal number
     * of replicas reported from data-nodes.
     */
    public boolean commitOrCompleteLastBlock(INodeFile fileINode,
            Block commitBlock) throws IOException, PersistanceException {
        assert fileINode.isUnderConstruction();
        if (commitBlock == null) {
            return false; // not committing, this is a block allocation retry
        }
        BlockInfo lastBlock = fileINode.getLastBlock();

        if (lastBlock == null) {
            return false; // no blocks in file yet
        }
        if (lastBlock.isComplete()) {
            return false; // already completed (e.g. by syncBlock) 
        }

        final boolean b = commitBlock((BlockInfoUnderConstruction) lastBlock, commitBlock);

        if (countNodes(lastBlock).liveReplicas() >= minReplication) {
            completeBlock(fileINode, fileINode.numBlocks() - 1);
        }
        return b;
    }
    
    
    public BlockInfo kthfsTryToCompleteBlock(final INodeFile fileINode,
            final int blkIndex) throws IOException, PersistanceException
    {
        if (blkIndex < 0) {
            return null;
        }
        
        BlockInfo curBlock = fileINode.getBlocks().get(blkIndex);
        if (curBlock.isComplete()) {
           return curBlock;
        }
        
        BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) curBlock;
        if (ucBlock.getReplicas().size() < minReplication) {
            return null;
        }
        BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
        // replace penultimate block in file
        fileINode.setBlock(blkIndex, completeBlock);
        completeBlock.setINode(fileINode);
        EntityManager.update(completeBlock);
        for (ReplicaUnderConstruction rep : ucBlock.getExpectedReplicas()) {
            EntityManager.remove(rep);
        }
        ucBlock.getExpectedReplicas().clear();
        return completeBlock;
    }

    /**
     * Convert a specified block of the file to a complete block.
     *
     * @param fileINode file
     * @param blkIndex block index in the file
     * @throws IOException if the block does not have at least a minimal number
     * of replicas reported from data-nodes.
     */
    private BlockInfo completeBlock(final INodeFile fileINode,
            final int blkIndex) throws IOException, PersistanceException {

        if (blkIndex < 0) {
            return null;
        }
        BlockInfo curBlock = fileINode.getBlocks().get(blkIndex);
        if (curBlock.isComplete()) {
            return curBlock;
        }
        BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) curBlock;
        if (ucBlock.getReplicas().size() < minReplication) {
            throw new IOException("Cannot complete block: "
                    + "block does not satisfy minimal replication requirement.");
        }
        BlockInfo completeBlock = ucBlock.convertToCompleteBlock();
        // replace penultimate block in file
        fileINode.setBlock(blkIndex, completeBlock);
        completeBlock.setINode(fileINode);
        EntityManager.update(completeBlock);

        for (ReplicaUnderConstruction rep : ucBlock.getExpectedReplicas()) {
            EntityManager.remove(rep);
        }

        ucBlock.getExpectedReplicas().clear();

        return completeBlock;
    }

    private BlockInfo completeBlock(final INodeFile fileINode,
            final BlockInfo block) throws IOException, PersistanceException {

        List<BlockInfo> fileBlocks = fileINode.getBlocks();
        for (int idx = 0; idx < fileBlocks.size(); idx++) {
            if (fileBlocks.get(idx).getBlockId() == block.getBlockId()) {
                return completeBlock(fileINode, idx);
            }
        }
        return block;
    }

    /**
     * Convert the last block of the file to an under construction block.<p> The
     * block is converted only if the file has blocks and the last one is a
     * partial block (its size is less than the preferred block size). The
     * converted block is returned to the client. The client uses the returned
     * block locations to form the data pipeline for this block.<br> The methods
     * returns null if there is no partial block at the end. The client is
     * supposed to allocate a new block with the next call.
     *
     * @param fileINode file
     * @return the last block locations if the block is partial or null
     * otherwise
     */
    public LocatedBlock convertLastBlockToUnderConstruction(
            INodeFile fileINode) throws IOException, PersistanceException {
        assert fileINode.isUnderConstruction();
        BlockInfo oldBlock = fileINode.getLastBlock();
        if (oldBlock == null
                || fileINode.getPreferredBlockSize() == oldBlock.getNumBytes()) {
            return null;
        }
        LOG.debug("oldBlock=" + oldBlock);
        LOG.debug("getStoredBlock(oldBlock)=" + getStoredBlock(oldBlock));
        assert oldBlock.equals(getStoredBlock(oldBlock)) :
                "last block of the file is not in blocksMap";

        BlockInfoUnderConstruction ucBlock = fileINode.setLastBlock(oldBlock);
        EntityManager.update(ucBlock);

        for (IndexedReplica replica : oldBlock.getReplicas()) {
            ReplicaUnderConstruction addedReplica = ucBlock.addExpectedReplica(replica.getStorageId(), HdfsServerConstants.ReplicaState.RBW);

            if (addedReplica != null) {
                EntityManager.add(addedReplica);
            }
        }

        // Remove block from replication queue.
        updateNeededReplications(oldBlock, 0, 0);

        // remove this block from the list of pending blocks to be deleted. 
        for (IndexedReplica replica : oldBlock.getReplicas()) {
            if (invalidateBlocks.contains(replica.getStorageId(), oldBlock)) {
                invalidateBlocks.remove(replica.getStorageId(), oldBlock);
            }
        }

        final long fileLength = fileINode.computeContentSummary().getLength();
        final long pos = fileLength - ucBlock.getNumBytes();
        return createLocatedBlock(ucBlock, pos, BlockTokenSecretManager.AccessMode.WRITE);
    }

    /**
     * Get all valid locations of the block
     *
     * @throws IOException
     */
    private List<String> getValidLocations(Block block) throws IOException, PersistanceException {
        ArrayList<String> machineSet =
                new ArrayList<String>(getStoredBlock(block).getReplicas().size());
        List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(block));

        for (DatanodeDescriptor node : dataNodes) {
            String storageID = node.getStorageID();
            // filter invalidate replicas
            if (!invalidateBlocks.contains(storageID, block)) {
                machineSet.add(storageID);
            }
        }
        return machineSet;
    }

    private List<LocatedBlock> createLocatedBlockList(final List<BlockInfo> blocks,
            final long offset, final long length, final int nrBlocksToReturn,
            final BlockTokenSecretManager.AccessMode mode) throws IOException, PersistanceException {
        int curBlk = 0;
        long curPos = 0, blkSize = 0;
        int nrBlocks = (blocks.get(0).getNumBytes() == 0) ? 0 : blocks.size();
        for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
            blkSize = blocks.get(curBlk).getNumBytes();
            assert blkSize > 0 : "Block of size 0";
            if (curPos + blkSize > offset) {
                break;
            }
            curPos += blkSize;
        }

        if (nrBlocks > 0 && curBlk == nrBlocks) // offset >= end of file
        {
            return Collections.<LocatedBlock>emptyList();
        }

        long endOff = offset + length;
        List<LocatedBlock> results = new ArrayList<LocatedBlock>(blocks.size());
        do {
            results.add(createLocatedBlock(blocks.get(curBlk), curPos, mode));
            curPos += blocks.get(curBlk).getNumBytes();
            curBlk++;
        } while (curPos < endOff
                && curBlk < blocks.size()
                && results.size() < nrBlocksToReturn);
        return results;
    }

    private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos,
            final BlockTokenSecretManager.AccessMode mode) throws IOException, PersistanceException {
        final LocatedBlock lb;
        lb = createLocatedBlock(blk, pos);
        if (mode != null) {
            setBlockToken(lb, mode);
        }
        return lb;
    }

    /**
     * @return a LocatedBlock for the given block
     */
    private LocatedBlock createLocatedBlock(final BlockInfo blk, final long pos) throws IOException, PersistanceException {
        if (blk instanceof BlockInfoUnderConstruction) {
            if (blk.isComplete()) {
                throw new IOException(
                        "blk instanceof BlockInfoUnderConstruction && blk.isComplete()"
                        + ", blk=" + blk);
            }
            final BlockInfoUnderConstruction uc = (BlockInfoUnderConstruction) blk;
            final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
            return new LocatedBlock(eb, getExpectedDatanodes(uc), pos, false);
        }
        // get block locations
        final int numCorruptNodes = countNodes(blk).corruptReplicas();
        final int numCorruptReplicas = numCorruptReplicas(blk);
        if (numCorruptNodes != numCorruptReplicas) {
            LOG.warn("Inconsistent number of corrupt replicas for "
                    + blk + " blockMap has " + numCorruptNodes
                    + " but corrupt replicas map has " + numCorruptReplicas);
        }

        final int numNodes = getStoredBlock(blk).getReplicas().size();
        final boolean isCorrupt = numCorruptNodes == numNodes;
        final int numMachines = isCorrupt ? numNodes : numNodes - numCorruptNodes;
        final DatanodeDescriptor[] machines = new DatanodeDescriptor[numMachines];
        if (numMachines > 0) {
            int j = 0;
            List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(blk));

            for (DatanodeDescriptor d : dataNodes) {
                final boolean replicaCorrupt = EntityManager.find(CorruptReplica.Finder.ByPk, blk.getBlockId(), d.getStorageID()) != null;
                if (isCorrupt || (!isCorrupt && !replicaCorrupt)) {
                    machines[j++] = d;
                }
            }
        }
        final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
        return new LocatedBlock(eb, machines, pos, isCorrupt);
    }

    /**
     * Create a LocatedBlocks.
     */
    public LocatedBlocks createLocatedBlocks(final List<BlockInfo> blocks,
            final long fileSizeExcludeBlocksUnderConstruction,
            final boolean isFileUnderConstruction,
            final long offset, final long length, final boolean needBlockToken) throws IOException, PersistanceException {
        //assert namesystem.hasReadOrWriteLock();
        if (blocks == null) {
            return null;
        } else if (blocks.isEmpty()) {
            return new LocatedBlocks(0, isFileUnderConstruction,
                    Collections.<LocatedBlock>emptyList(), null, false);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("blocks = " + java.util.Arrays.asList(blocks));
            }
            final BlockTokenSecretManager.AccessMode mode = needBlockToken ? BlockTokenSecretManager.AccessMode.READ : null;
            final List<LocatedBlock> locatedblocks = createLocatedBlockList(
                    blocks, offset, length, Integer.MAX_VALUE, mode);
            final BlockInfo last = blocks.get(blocks.size() - 1);
            final long lastPos = last.isComplete()
                    ? fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
                    : fileSizeExcludeBlocksUnderConstruction;
            final LocatedBlock lastlb = createLocatedBlock(last, lastPos, mode);
            return new LocatedBlocks(
                    fileSizeExcludeBlocksUnderConstruction, isFileUnderConstruction,
                    locatedblocks, lastlb, last.isComplete());
        }
    }

    /**
     * @return current access keys.
     */
    public ExportedBlockKeys getBlockKeys() {
        return isBlockTokenEnabled() ? blockTokenSecretManager.exportKeys()
                : ExportedBlockKeys.DUMMY_KEYS;
    }

    /**
     * Generate a block token for the located block.
     */
    public void setBlockToken(final LocatedBlock b,
            final BlockTokenSecretManager.AccessMode mode) throws IOException {
        if (isBlockTokenEnabled()) {
            b.setBlockToken(blockTokenSecretManager.generateToken(b.getBlock(),
                    EnumSet.of(mode)));
        }
    }

    void addKeyUpdateCommand(final List<DatanodeCommand> cmds,
            final DatanodeDescriptor nodeinfo) {
        // check access key update
        if (isBlockTokenEnabled() && nodeinfo.needKeyUpdate) {
            cmds.add(new KeyUpdateCommand(blockTokenSecretManager.exportKeys()));
            nodeinfo.needKeyUpdate = false;
        }
    }

    /**
     * Clamp the specified replication between the minimum and the maximum
     * replication levels.
     */
    public short adjustReplication(short replication) {
        return replication < minReplication ? minReplication
                : replication > maxReplication ? maxReplication : replication;
    }

    /**
     * Check whether the replication parameter is within the range determined by
     * system configuration.
     */
    public void verifyReplication(String src,
            short replication,
            String clientName) throws IOException {

        if (replication >= minReplication && replication <= maxReplication) {
            //common case. avoid building 'text'
            return;
        }

        String text = "file " + src
                + ((clientName != null) ? " on client " + clientName : "")
                + ".\n"
                + "Requested replication " + replication;

        if (replication > maxReplication) {
            throw new IOException(text + " exceeds maximum " + maxReplication);
        }

        if (replication < minReplication) {
            throw new IOException(text + " is less than the required minimum "
                    + minReplication);
        }
    }

    /**
     * return a list of blocks & their locations on
     * <code>datanode</code> whose total size is
     * <code>size</code>
     *
     * @param datanode on which blocks are located
     * @param size total size of blocks
     */
    public BlocksWithLocations getBlocks(DatanodeID datanode, long size) throws IOException, PersistanceException {
        namesystem.readLock();
        try {
            namesystem.checkSuperuserPrivilege();
            return getBlocksWithLocations(datanode, size);
        } finally {
            namesystem.readUnlock();
        }
    }

    /**
     * Get all blocks with location information from a datanode.
     *
     * @throws IOException
     */
    private BlocksWithLocations getBlocksWithLocations(final DatanodeID datanode,
            final long size) throws IOException, PersistanceException {
        final DatanodeDescriptor node = getDatanodeManager().getDatanode(datanode);
        if (node == null) {
            NameNode.stateChangeLog.warn("BLOCK* getBlocks: "
                    + "Asking for blocks from an unrecorded node " + datanode.getName());
            throw new HadoopIllegalArgumentException(
                    "Datanode " + datanode.getName() + " not found.");
        }

        int numBlocks = node.numBlocks();
        if (numBlocks == 0) {
            return new BlocksWithLocations(new BlockWithLocations[0]);
        }
        Iterator<BlockInfo> iter = EntityManager.findList(BlockInfo.Finder.ByStorageId, node.getStorageID()).iterator();
        int startBlock = DFSUtil.getRandom().nextInt(numBlocks); // starting from a random block
        // skip blocks
        for (int i = 0; i < startBlock; i++) {
            iter.next();
        }
        List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
        long totalSize = 0;
        BlockInfo curBlock;
        while (totalSize < size && iter.hasNext()) {
            curBlock = iter.next();
            if (!curBlock.isComplete()) {
                continue;
            }
            totalSize += addBlock(curBlock, results);
        }
        if (totalSize < size) {
            iter = EntityManager.findList(BlockInfo.Finder.ByStorageId, node.getStorageID()).iterator();
            for (int i = 0; i < startBlock && totalSize < size; i++) {
                curBlock = iter.next();
                if (!curBlock.isComplete()) {
                    continue;
                }
                totalSize += addBlock(curBlock, results);
            }
        }

        return new BlocksWithLocations(
                results.toArray(new BlockWithLocations[results.size()]));
    }

    /**
     * Remove the blocks associated to the given datanode.
     *
     * @throws IOException
     */
    void removeBlocksAssociatedTo(final DatanodeDescriptor node, OperationType opType) throws IOException {
        final Iterator<? extends Block> it = findBlocksAssociatedTo(node.getStorageID(), opType);

        while (it.hasNext()) {
            removeBlock(it.next(), node, opType);
        }

        node.resetBlocks();
        invalidateBlocks.remove(node.getStorageID(), opType);
    }

    private Iterator<? extends Block> findBlocksAssociatedTo(final String sid, final OperationType opType) throws IOException {
        LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                BlockInfoDataAccess da = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
                return da.findByStorageId(sid).iterator();
            }
        };
        return (Iterator<? extends Block>) findBlocksHandler.handle();
    }

    private void removeBlock(final Block b, final DatanodeDescriptor node, OperationType opType) throws IOException {
        TransactionalRequestHandler removeBlockHandler = new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                removeStoredBlock(b, node);
                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, UnresolvedPathException {
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.READ).
                        addBlock(LockType.WRITE, b.getBlockId()).
                        addReplica(LockType.WRITE).
                        addExcess(LockType.WRITE).
                        addCorrupt(LockType.WRITE).
                        addUnderReplicatedBlock(LockType.WRITE).
                        addReplicaUc(LockType.WRITE);
                
                lm.acquireByBlock();
            }
        };
        removeBlockHandler.setParams(node).handle();
    }

    /**
     * Adds block to list of blocks which will be invalidated on specified
     * datanode and log the operation
     */
    void addToInvalidates(final Block block, final DatanodeInfo datanode) throws PersistanceException {
        invalidateBlocks.add(block, datanode, true);
    }

    /**
     * Adds block to list of blocks which will be invalidated on all its
     * datanodes.
     *
     * @throws IOException
     */
    private void addToInvalidates(Block b) throws IOException, PersistanceException {
        StringBuilder datanodes = new StringBuilder();
        List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(b));

        for (DatanodeDescriptor node : dataNodes) {
            invalidateBlocks.add(b, node, false);
            datanodes.append(node.getName()).append(" ");
        }
        if (datanodes.length() != 0) {
            NameNode.stateChangeLog.info("BLOCK* addToInvalidates: "
                    + b + " to " + datanodes.toString());
        }
    }

    /**
     * Mark the block belonging to datanode as corrupt
     *
     * @param blk Block to be marked as corrupt
     * @param dn Datanode which holds the corrupt replica
     */
    public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
            final DatanodeInfo dn) throws IOException {
        TransactionalRequestHandler findAndMarkBlockAsCorruptHandler = new TransactionalRequestHandler(OperationType.FIND_AND_MARK_BLOCKS_AS_CORRUPT) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                final BlockInfo storedBlock = getStoredBlock(blk.getLocalBlock());
                if (storedBlock == null) {
                    // Check if the replica is in the blockMap, if not
                    // ignore the request for now. This could happen when BlockScanner
                    // thread of Datanode reports bad block before Block reports are sent
                    // by the Datanode on startup
                    NameNode.stateChangeLog.info("BLOCK* findAndMarkBlockAsCorrupt: "
                            + blk + " not found.");
                    return null;
                }
                markBlockAsCorrupt(storedBlock, dn);
                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.READ).
                        addBlock(LockType.WRITE, blk.getBlockId()).
                        addReplica(LockType.READ).
                        addExcess(LockType.WRITE).
                        addCorrupt(LockType.WRITE).
                        addUnderReplicatedBlock(LockType.WRITE).
                        addReplicaUc(LockType.READ);
                lm.acquireByBlock();
            }
        };
        findAndMarkBlockAsCorruptHandler.handleWithWriteLock(namesystem);
    }

    private void markBlockAsCorrupt(BlockInfo storedBlock,
            DatanodeInfo dn) throws IOException, PersistanceException {
        assert storedBlock != null : "storedBlock should not be null";
        DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
        if (node == null) {
            throw new IOException("Cannot mark block "
                    + storedBlock.getBlockName()
                    + " as corrupt because datanode " + dn.getName()
                    + " does not exist. ");
        }

        INodeFile inode = storedBlock.getINode();
        if (inode == null) {
            NameNode.stateChangeLog.info("BLOCK markBlockAsCorrupt: "
                    + "block " + storedBlock
                    + " could not be marked as corrupt as it"
                    + " does not belong to any file");
            addToInvalidates(storedBlock, node);
            return;
        }
        // Add replica to the data-node if it is not already there
        IndexedReplica replica = storedBlock.addReplica(node);
        if (replica != null) {
            EntityManager.add(replica);
        }

        // Add this replica to corruptReplicas Map
        EntityManager.add(new CorruptReplica(storedBlock.getBlockId(), node.getStorageID()));
        if (countNodes(storedBlock).liveReplicas() > inode.getReplication()) {
            // the block is over-replicated so invalidate the replicas immediately
            invalidateBlock(storedBlock, node);
        } else if (namesystem.isPopulatingReplQueues()) {
            // add the block to neededReplication
            updateNeededReplications(storedBlock, -1, 0);
        }
    }

    /**
     * Invalidates the given block on the given datanode.
     */
    private void invalidateBlock(Block blk, DatanodeInfo dn)
            throws IOException, PersistanceException {
        NameNode.stateChangeLog.info("BLOCK* invalidateBlock: "
                + blk + " on " + dn.getName());
        DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
        if (node == null) {
            throw new IOException("Cannot invalidate block " + blk
                    + " because datanode " + dn.getName() + " does not exist.");
        }

        // Check how many copies we have of the block. If we have at least one
        // copy on a live node, then we can delete it.
        int count = countNodes(blk).liveReplicas();
        if (count > 1) {
            addToInvalidates(blk, dn);
            removeStoredBlock(blk, node);
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("BLOCK* invalidateBlocks: "
                        + blk + " on " + dn.getName() + " listed for deletion.");
            }
        } else {
            NameNode.stateChangeLog.info("BLOCK* invalidateBlocks: " + blk + " on "
                    + dn.getName() + " is the only copy and was not deleted.");
        }
    }

    void updateState(OperationType opType) throws IOException {
        pendingReplicationBlocksCount = pendingReplications.size(opType);
        underReplicatedBlocksCount = neededReplications.size(opType);
        corruptReplicaBlocksCount = countCorruptReplica(opType);
    }

    private int countCorruptReplica(OperationType opType) throws IOException {
        return (Integer) new LightWeightRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                CorruptReplicaDataAccess da = (CorruptReplicaDataAccess) StorageFactory.getDataAccess(CorruptReplicaDataAccess.class);
                Collection result = da.findAll();
                if (result != null) {
                    return result.size();
                }
                return 0;
            }
        }.handle();
    }

    /**
     * Return number of under-replicated but not missing blocks
     */
    public int getUnderReplicatedNotMissingBlocks(OperationType opType) throws IOException {
        return neededReplications.getUnderReplicatedBlockCount(opType);
    }

    /**
     * Schedule blocks for deletion at datanodes
     *
     * @param nodesToProcess number of datanodes to schedule deletion work
     * @return total number of block for deletion
     */
    int computeInvalidateWork(int nodesToProcess, OperationType opType) throws IOException {

        final List<String> nodes = getStorageIdsOfIvalidatedBlocks(opType);
        Collections.shuffle(nodes);

        nodesToProcess = Math.min(nodes.size(), nodesToProcess);

        int blockCnt = 0;
        for (int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++) {
            blockCnt += invalidateWorkForOneNode(nodes.get(nodeCnt), opType);
        }
        return blockCnt;
    }

  private List<String> getStorageIdsOfIvalidatedBlocks(OperationType opType) throws IOException {
    return invalidateBlocks.getStorageIDs(opType);
  }

    /**
     * Scan blocks in {@link #neededReplications} and assign replication work to
     * data-nodes they belong to.
     *
     * The number of process blocks equals either twice the number of live
     * data-nodes or the number of under-replicated blocks whichever is less.
     *
     * @return number of blocks scheduled for replication during this iteration.
     */
    private int computeReplicationWork(int blocksToProcess, OperationType opType) throws IOException {
        // Choose the blocks to be replicated
        List<List<Block>> blocksToReplicate =
                chooseUnderReplicatedBlocks(blocksToProcess, opType);
        // replicate blocks
        int scheduledReplicationCount = 0;
        for (int i = 0; i < blocksToReplicate.size(); i++) {
            for (Block block : blocksToReplicate.get(i)) {
                if (computeReplicationWorkForBlock(block, i, opType)) {
                    scheduledReplicationCount++;
                }
            }
        }
        return scheduledReplicationCount;
    }

    private boolean computeReplicationWorkForBlock(final Block b, final int priority, OperationType opType) throws IOException {
        TransactionalRequestHandler computeReplicationWorkHandler = new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                return computeReplicationWorkForBlock(b, priority);
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.WRITE).
                        addBlock(LockType.WRITE, b.getBlockId()).
                        addReplica(LockType.READ).
                        addExcess(LockType.READ).
                        addCorrupt(LockType.READ).
                        addPendingBlock(LockType.READ).
                        addUnderReplicatedBlock(LockType.WRITE).
                        addReplicaUc(LockType.READ);
                lm.acquireByBlock();
            }
        };
        return (Boolean) computeReplicationWorkHandler.handle();
    }

    private Collection<UnderReplicatedBlock> getUnderReplicatedBlocks(OperationType opType) throws IOException {
        TransactionalRequestHandler findAllUrbHander = new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                return EntityManager.findList(UnderReplicatedBlock.Finder.All);
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                TransactionLockAcquirer.acquireLockList(LockType.READ_COMMITTED,
                        UnderReplicatedBlock.Finder.All, null);
            }
        };
        return (Collection<UnderReplicatedBlock>) findAllUrbHander.handle();
    }

    /**
     * Get a list of block lists to be replicated The index of block lists
     * represents the
     *
     * @param blocksToProcess
     * @return Return a list of block lists to be replicated. The block list
     * index represents its replication priority.
     */
    private List<List<Block>> chooseUnderReplicatedBlocks(int blocksToProcess, OperationType opType) throws IOException {
        // initialize data structure for the return value
        final List<List<Block>> blocksToReplicate = new ArrayList<List<Block>>(
                UnderReplicatedBlocks.LEVEL);
        for (int i = 0; i < UnderReplicatedBlocks.LEVEL; i++) {
            blocksToReplicate.add(new ArrayList<Block>());
        }
        namesystem.writeLock();
        try {
            synchronized (neededReplications) {

//        if (neededReplications.size() == 0) {
//          return blocksToReplicate;
//        }

                Collection<UnderReplicatedBlock> urblocks = getUnderReplicatedBlocks(opType);
                if (urblocks.isEmpty()) {
                    return blocksToReplicate;
                }

                Iterator<UnderReplicatedBlock> iterator = urblocks.iterator();
                int urbSize = urblocks.size();
                // skip to the first unprocessed block, which is at replIndex
                for (int i = 0; i < replIndex && iterator.hasNext(); i++) {
                    iterator.next();
                }
                // Go through all blocks that need replications.
                // # of blocks to process equals either twice the number of live
                // data-nodes or the number of under-replicated blocks whichever is less
                blocksToProcess = Math.min(blocksToProcess, urbSize);

                for (int blkCnt = 0; blkCnt < blocksToProcess; blkCnt++, replIndex++) {
                    if (!iterator.hasNext()) {
                        // start from the beginning
                        replIndex = 0;
                        blocksToProcess = Math.min(blocksToProcess, urbSize);
                        if (blkCnt >= blocksToProcess) {
                            break;
                        }
                        iterator = urblocks.iterator();
                        assert iterator.hasNext() : "neededReplications should not be empty.";
                    }
                    UnderReplicatedBlock urb = iterator.next();
                    urbSize = findBlock(blocksToReplicate, urb, urbSize, opType);
                } // end for
            } // end synchronized neededReplication
        } finally {
            namesystem.writeUnlock();
        }

        return blocksToReplicate;
    }

    private int findBlock(final List<List<Block>> blocksToReplicate,
            final UnderReplicatedBlock urb, final int urbSize, OperationType opType) throws IOException {
        TransactionalRequestHandler findBlock = new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                Block block = EntityManager.find(BlockInfo.Finder.ById, urb.getBlockId());
                int priority = urb.getLevel();
                if (priority < 0 || priority >= blocksToReplicate.size()) {
                    LOG.warn("Unexpected replication priority: "
                            + priority + " " + block);
                } else {
                    if (block == null) // Block does not exist and should be removed from UnderReplicatedBlocks.
                    {
                        neededReplications.remove(new Block(urb.getBlockId()), priority);
                        return urbSize - 1;
                    } else {
                        blocksToReplicate.get(priority).add(block);
                    }
                }
                return urbSize;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                UnderReplicatedBlock urb = (UnderReplicatedBlock) getParams()[0];
                TransactionLockManager lm = new TransactionLockManager();
                lm.addBlock(LockType.WRITE, urb.getBlockId());
                lm.addUnderReplicatedBlock(LockType.WRITE);
                lm.acquire();
            }
        };
        return (Integer) findBlock.setParams(urb, urbSize).handle();
    }

    /**
     * Replicate a block
     *
     * @param block block to be replicated
     * @param priority a hint of its priority in the neededReplication queue
     * @return if the block gets replicated or not
     * @throws IOException
     */
    @VisibleForTesting
    boolean computeReplicationWorkForBlock(Block block, int priority) throws IOException, PersistanceException {
        int requiredReplication, numEffectiveReplicas;
        List<DatanodeDescriptor> containingNodes, liveReplicaNodes;
        DatanodeDescriptor srcNode;
        INodeFile fileINode = null;
        int additionalReplRequired;

        namesystem.writeLock();
        try {
            synchronized (neededReplications) {
                // block should belong to a file
                BlockInfo storedBlock = getStoredBlock(block);
                fileINode = (storedBlock != null) ? storedBlock.getINode() : null;
                // abandoned block or block reopened for append
                if (fileINode == null || fileINode.isUnderConstruction()) {
                    neededReplications.remove(block, priority); // remove from neededReplications
                    replIndex--;
                    return false;
                }

                requiredReplication = fileINode.getReplication();

                // get a source data-node
                containingNodes = new ArrayList<DatanodeDescriptor>();
                liveReplicaNodes = new ArrayList<DatanodeDescriptor>();
                NumberReplicas numReplicas = new NumberReplicas();
                srcNode = chooseSourceDatanode(
                        block, containingNodes, liveReplicaNodes, numReplicas);
                if (srcNode == null) // block can not be replicated from any node
                {
                    return false;
                }

                assert liveReplicaNodes.size() == numReplicas.liveReplicas();
                // do not schedule more if enough replicas is already pending
                numEffectiveReplicas = numReplicas.liveReplicas()
                        + pendingReplications.getNumReplicas(block);

                if (numEffectiveReplicas >= requiredReplication) {
                    if ((pendingReplications.getNumReplicas(block) > 0)
                            || (blockHasEnoughRacks(block))) {
                        neededReplications.remove(block, priority); // remove from neededReplications
                        replIndex--;
                        NameNode.stateChangeLog.info("BLOCK* "
                                + "Removing block " + block
                                + " from neededReplications as it has enough replicas.");
                        return false;
                    }
                }

                if (numReplicas.liveReplicas() < requiredReplication) {
                    additionalReplRequired = requiredReplication - numEffectiveReplicas;
                } else {
                    additionalReplRequired = 1; //Needed on a new rack
                }

            }
        } finally {
            namesystem.writeUnlock();
        }

        // Exclude all of the containing nodes from being targets.
        // This list includes decommissioning or corrupt nodes.
        HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
        for (DatanodeDescriptor dn : containingNodes) {
            excludedNodes.put(dn, dn);
        }

        // choose replication targets: NOT HOLDING THE GLOBAL LOCK
        // It is costly to extract the filename for which chooseTargets is called,
        // so for now we pass in the Inode itself.
        DatanodeDescriptor targets[] =
                blockplacement.chooseTarget(fileINode, additionalReplRequired,
                srcNode, liveReplicaNodes, excludedNodes, block.getNumBytes());
        if (targets.length == 0) {
            return false;
        }

        namesystem.writeLock();
        try {
            synchronized (neededReplications) {
                // Recheck since global lock was released
                // block should belong to a file
                fileINode = getINode(block);
                // abandoned block or block reopened for append
                if (fileINode == null || fileINode.isUnderConstruction()) {
                    neededReplications.remove(block, priority); // remove from neededReplications
                    replIndex--;
                    return false;
                }
                requiredReplication = fileINode.getReplication();

                // do not schedule more if enough replicas is already pending
                NumberReplicas numReplicas = countNodes(block);
                numEffectiveReplicas = numReplicas.liveReplicas()
                        + pendingReplications.getNumReplicas(block);

                if (numEffectiveReplicas >= requiredReplication) {
                    if ((pendingReplications.getNumReplicas(block) > 0)
                            || (blockHasEnoughRacks(block))) {
                        neededReplications.remove(block, priority); // remove from neededReplications
                        replIndex--;
                        NameNode.stateChangeLog.info("BLOCK* "
                                + "Removing block " + block
                                + " from neededReplications as it has enough replicas.");
                        return false;
                    }
                }

                if ((numReplicas.liveReplicas() >= requiredReplication)
                        && (!blockHasEnoughRacks(block))) {
                    if (srcNode.getNetworkLocation().equals(targets[0].getNetworkLocation())) {
                        //No use continuing, unless a new rack in this case
                        return false;
                    }
                }

                // Add block to the to be replicated list
                srcNode.addBlockToBeReplicated(block, targets);

                for (DatanodeDescriptor dn : targets) {
                    dn.incBlocksScheduled();
                }

                // Move the block-replication into a "pending" state.
                // The reason we use 'pending' is so we can retry
                // replications that fail after an appropriate amount of time.
                pendingReplications.add(block, targets.length);
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug(
                            "BLOCK* block " + block
                            + " is moved from neededReplications to pendingReplications");
                }

                // remove from neededReplications
                // -------------------------------------------------------------------------
                if (numEffectiveReplicas + targets.length >= requiredReplication) {
                    neededReplications.remove(block, priority); // remove from neededReplications
                    replIndex--;
                }
                if (NameNode.stateChangeLog.isInfoEnabled()) {
                    StringBuilder targetList = new StringBuilder("datanode(s)");
                    for (int k = 0; k < targets.length; k++) {
                        targetList.append(' ');
                        targetList.append(targets[k].getName());
                    }
                    NameNode.stateChangeLog.info(
                            "BLOCK* ask "
                            + srcNode.getName() + " to replicate "
                            + block + " to " + targetList);
                    // TODO HOOMAN[lock]
//          if (NameNode.stateChangeLog.isDebugEnabled()) {
//            NameNode.stateChangeLog.debug(
//                    "BLOCK* neededReplications = " + neededReplications.size()
//                    + " pendingReplications = " + pendingReplications.size());
//          }
                }
            }
        } finally {
            namesystem.writeUnlock();
        }

        return true;
    }

    /**
     * Choose target datanodes according to the replication policy.
     *
     * @throws IOException if the number of targets < minimum replication.
     * @see BlockPlacementPolicy#chooseTarget(String, int, DatanodeDescriptor,
     * HashMap, long)
     */
    public DatanodeDescriptor[] chooseTarget(final String src,
            final int numOfReplicas, final DatanodeDescriptor client,
            final HashMap<Node, Node> excludedNodes,
            final long blocksize) throws IOException {
        
          //[S] DFSOutputStream has a list of suspected datanodes
          //it sends the list of suspected dead datanodes 
          //there is no need to add them again as correct nodes. 
        
//        //Kamal: this was added for the caught bug for removed nodes in NetworkTopology
//        if (excludedNodes != null) {
//            Iterator<Node> iterator = excludedNodes.values().iterator();
//            while (iterator.hasNext()) {
//                Node node = iterator.next();
//                if (datanodeManager.getDatanodeByName(node.getName()) == null) {
//                    excludedNodes.remove(node);
//                }
//            }
//        }
        // choose targets for the new block to be allocated.
        final DatanodeDescriptor targets[] = blockplacement.chooseTarget(
                src, numOfReplicas, client, excludedNodes, blocksize);
        if (targets.length < minReplication) {
            throw new IOException("File " + src + " could only be replicated to "
                    + targets.length + " nodes instead of minReplication (="
                    + minReplication + ").  There are "
                    + getDatanodeManager().getNetworkTopology().getNumOfLeaves()
                    + " datanode(s) running and "
                    + (excludedNodes == null ? "no" : excludedNodes.size())
                    + " node(s) are excluded in this operation.");
        }
        return targets;
    }

    /**
     * Parse the data-nodes the block belongs to and choose one, which will be
     * the replication source.
     *
     * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
     * since the former do not have write traffic and hence are less busy. We do
     * not use already decommissioned nodes as a source. Otherwise we choose a
     * random node among those that did not reach their replication limit.
     *
     * In addition form a list of all nodes containing the block and calculate
     * its replication numbers.
     *
     * @throws IOException
     */
    private DatanodeDescriptor chooseSourceDatanode(
            final Block block,
            List<DatanodeDescriptor> containingNodes,
            List<DatanodeDescriptor> nodesContainingLiveReplicas,
            NumberReplicas numReplicas) throws IOException, PersistanceException {
        containingNodes.clear();
        nodesContainingLiveReplicas.clear();
        DatanodeDescriptor srcNode = null;
        int live = 0;
        int decommissioned = 0;
        int corrupt = 0;
        int excess = 0;
        List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(block));

        for (DatanodeDescriptor node : dataNodes) {
            Collection<ExcessReplica> excessBlocks =
                    EntityManager.findList(ExcessReplica.Finder.ByBlockId, block.getBlockId());
            if (isItCorruptedReplica(block.getBlockId(), node.getStorageID())) {
                corrupt++;
            } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
                decommissioned++;
            } else if (excessBlocks != null
                    && excessBlocks.contains(new ExcessReplica(node.getStorageID(), block.getBlockId()))) {
                excess++;
            } else {
                nodesContainingLiveReplicas.add(node);
                live++;
            }
            containingNodes.add(node);
            // Check if this replica is corrupt
            // If so, do not select the node as src node
            if (isItCorruptedReplica(block.getBlockId(), node.getStorageID())) {
                continue;
            }
            if (node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams) {
                continue; // already reached replication limit
            }      // the block must not be scheduled for removal on srcNode
            if (excessBlocks != null
                    && excessBlocks.contains(new ExcessReplica(node.getStorageID(), block.getBlockId()))) {
                continue;
            }
            // never use already decommissioned nodes
            if (node.isDecommissioned()) {
                continue;
            }
            // we prefer nodes that are in DECOMMISSION_INPROGRESS state
            if (node.isDecommissionInProgress() || srcNode == null) {
                srcNode = node;
                continue;
            }
            if (srcNode.isDecommissionInProgress()) {
                continue;
            }
            // switch to a different node randomly
            // this to prevent from deterministically selecting the same node even
            // if the node failed to replicate the block on previous iterations
            if (DFSUtil.getRandom().nextBoolean()) {
                srcNode = node;
            }
        }
        if (numReplicas != null) {
            numReplicas.initialize(live, decommissioned, corrupt, excess);
        }
        return srcNode;
    }

    /**
     * If there were any replication requests that timed out, reap them and put
     * them back into the neededReplication queue
     *
     * @throws IOException
     */
    private void processPendingReplications(OperationType opType) throws IOException {
        List<PendingBlockInfo> timedoutPendings = pendingReplications.getTimedOutBlocks(opType);
        if (timedoutPendings != null) {
            namesystem.writeLock();
            try {
                for (int i = 0; i < timedoutPendings.size(); i++) {
                    PendingBlockInfo p = timedoutPendings.get(i);
                    processTimedOutPendingBlock(p, opType);
                }
            } finally {
                namesystem.writeUnlock();
            }
            /*
             * If we know the target datanodes where the replication timedout, we
             * could invoke decBlocksScheduled() on it. Its ok for now.
             */

        }
    }

    private void processTimedOutPendingBlock(final PendingBlockInfo p, OperationType opType) throws IOException {
        new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                // [lock]: Validation for pending block
                PendingBlockInfo pendingBlock = EntityManager.find(PendingBlockInfo.Finder.ByPKey, p.getBlockId());
                if (pendingBlock != null && PendingReplicationBlocks.isTimedOut(pendingBlock)) {
                    Block timedOutItem = EntityManager.find(BlockInfo.Finder.ById, p.getBlockId());
                    NumberReplicas num = countNodes(timedOutItem);
                    if (isNeededReplication(timedOutItem, getReplication(timedOutItem),
                            num.liveReplicas())) {
                        neededReplications.add(timedOutItem,
                                num.liveReplicas(),
                                num.decommissionedReplicas(),
                                getReplication(timedOutItem));
                    }
                }
                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.READ).
                        addBlock(LockType.WRITE).
                        addReplica(LockType.READ).
                        addExcess(LockType.READ).
                        addCorrupt(LockType.READ).
                        addPendingBlock(LockType.READ).
                        addUnderReplicatedBlock(LockType.WRITE);
                lm.acquireByBlock();
            }
        }.handle();
    }

    /**
     * StatefulBlockInfo is used to build the "toUC" list, which is a list of
     * updates to the information about under-construction blocks. Besides the
     * block in question, it provides the ReplicaState reported by the datanode
     * in the block report.
     */
    private static class StatefulBlockInfo {

        final BlockInfoUnderConstruction storedBlock;
        final ReplicaState reportedState;

        StatefulBlockInfo(BlockInfoUnderConstruction storedBlock,
                ReplicaState reportedState) {
            this.storedBlock = storedBlock;
            this.reportedState = reportedState;
        }
    }

    private boolean prepareProcessReport(final DatanodeDescriptor node, final DatanodeID nodeID,
            final List<BlockInfo> existingBlocks) throws IOException {
        // To minimize startup time, we discard any second (or later) block reports
        // that we receive while still in startup phase.
        if (isInStartUpSafeMode(OperationType.PREPARE_PROCESS_REPORT) && node.numBlocks() > 0) {
            NameNode.stateChangeLog.info("BLOCK* processReport: "
                    + "discarded non-initial block report from " + nodeID.getName()
                    + " because namenode still in startup phase");
            return false;
        }

        LightWeightRequestHandler findBlocksHandler = new LightWeightRequestHandler(OperationType.PREPARE_PROCESS_REPORT) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                BlockInfoDataAccess da = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
                return da.findByStorageId(node.getStorageID());
            }
        };

        if (node.numBlocks() > 0) { // If it's not the first block report of this datanode.
            existingBlocks.addAll((Collection<BlockInfo>) findBlocksHandler.handle());
        }

        return true;
    }

    private boolean isInStartUpSafeMode(final OperationType opType) throws IOException {
        return (Boolean) new TransactionalRequestHandler(opType) {
            @Override
            public void acquireLock() throws PersistanceException, IOException {
                // safe-mode
            }

            @Override
            public Object performTask() throws PersistanceException, IOException {
                return namesystem.isInStartupSafeMode();
            }
        }.handle();
    }

    /**
     * The given datanode is reporting all its blocks. Update the
     * (machine-->blocklist) and (block-->machinelist) maps.
     */
    public void processReport(final DatanodeID nodeID, final String poolId,
            final BlockListAsLongs newReport) throws IOException {
        namesystem.writeLock();
        try {
            long startTime = 0;
            long endTime = 0;

            startTime = Util.now();

            final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
            if (node == null || !node.isAlive) {
                throw new IOException("ProcessReport from dead or unregistered node: "
                        + nodeID.getName());
            }

            final List<BlockInfo> existingBlocks = new ArrayList<BlockInfo>(); // The existingblocks is loaded and used if it's not the first block report of the datanode.

            if (!prepareProcessReport(node, nodeID, existingBlocks)) {
                return;
            }


            if (node.numBlocks() == 0) {
                // The first block report can be processed a lot more efficiently than
                // ordinary block reports.  This shortens restart times.
                processFirstBlockReport(node, newReport);
            } else {
                processReport(node, newReport, existingBlocks);
            }
            // Log the block report processing stats from Namenode perspective
            NameNode.getNameNodeMetrics().addBlockReport((int) (endTime - startTime));
            NameNode.stateChangeLog.info(
                    "BLOCK* processReport: from "
                    + nodeID.getName() + ", blocks: " + newReport.getNumberOfBlocks()
                    + ", processing time: " + (endTime - startTime) + " msecs");
        } finally {
            namesystem.writeUnlock();
        }
    }

    private void processReport(final DatanodeDescriptor node,
            BlockListAsLongs report, final List<BlockInfo> existingBlocks) throws IOException {
        if (report == null) {
            report = new BlockListAsLongs();
        }
        // scan the report and process newly reported blocks
        BlockReportIterator itBR = report.getBlockReportIterator();

        TransactionalRequestHandler processReportHandler = new TransactionalRequestHandler(OperationType.PROCESS_REPORT) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                Block iblk = (Block) getParams()[0];
                ReplicaState iState = (ReplicaState) getParams()[1];
                BlockInfo storedBlock = processReportedBlock(node, iblk, iState);

                // move block to the head of the list
                if (storedBlock != null && storedBlock.hasReplicaIn(node.getStorageID())) {
                    existingBlocks.remove(storedBlock);
                }
                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                Block iblk = (Block) getParams()[0];
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.WRITE).
                        addBlock(LockType.WRITE, iblk.getBlockId()).
                        addReplica(LockType.WRITE).
                        addCorrupt(LockType.WRITE).
                        addExcess(LockType.WRITE).
                        addReplicaUc(LockType.WRITE).
                        addUnderReplicatedBlock(LockType.WRITE).
                        addInvalidatedBlock(LockType.READ).
                        addPendingBlock(LockType.READ);
                lm.acquireByBlock();
            }
        };


        while (itBR.hasNext()) {
            Block iblk = itBR.next();
            ReplicaState iState = itBR.getCurrentReplicaState();
            processReportHandler.setParams(iblk, iState).handle();
        }

        TransactionalRequestHandler afterReportHandler = new TransactionalRequestHandler(OperationType.AFTER_PROCESS_REPORT) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                Block b = (Block) getParams()[0];
                removeStoredBlock(b, node);
                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                Block b = (Block) getParams()[0];
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.READ).
                        addBlock(LockType.WRITE, b.getBlockId()).
                        addReplica(LockType.WRITE).
                        addExcess(LockType.WRITE).
                        addCorrupt(LockType.WRITE).
                        addUnderReplicatedBlock(LockType.WRITE);
                lm.acquireByBlock();
            }
        };

        // collect blocks that have not been reported
        for (Block b : existingBlocks) {
            afterReportHandler.setParams(b);
            afterReportHandler.handle();
        }
    }

    /**
     * processFirstBlockReport is intended only for processing "initial" block
     * reports, the first block report received from a DN after it registers. It
     * just adds all the valid replicas to the datanode, without calculating a
     * toRemove list (since there won't be any). It also silently discards any
     * invalid blocks, thereby deferring their processing until the next block
     * report.
     *
     * @param node - DatanodeDescriptor of the node that sent the report
     * @param report - the initial block report, to be processed
     * @throws IOException
     */
    private void processFirstBlockReport(final DatanodeDescriptor node,
            final BlockListAsLongs report) throws IOException {
        if (report == null) {
            return;
        }
        assert (namesystem.hasWriteLock());
        assert (node.numBlocks() == 0);
        BlockReportIterator itBR = report.getBlockReportIterator();
        TransactionalRequestHandler processFirstBlockReportHandler = new TransactionalRequestHandler(OperationType.PROCESS_FIRST_BLOCK_REPORT) {
            @Override
            public Object performTask() throws PersistanceException, IOException {

                Block iblk = (Block) getParams()[0];
                ReplicaState reportedState = (ReplicaState) getParams()[1];

                BlockInfo storedBlock = getStoredBlock(iblk);
                // If block does not belong to any file, we are done.
                if (storedBlock == null) {
                    return null;
                }
                // If block is corrupt, mark it and continue to next block.
                BlockUCState ucState = storedBlock.getBlockUCState();
                if (isReplicaCorrupt(iblk, reportedState, storedBlock, ucState, node)) {
                    markBlockAsCorrupt(storedBlock, node);
                    return null;
                }

                // If block is under construction, add this replica to its list
                if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
                    ReplicaUnderConstruction expReplica =
                            ((BlockInfoUnderConstruction) storedBlock).addExpectedReplica(node.getStorageID(), reportedState);
                    if (expReplica != null) {
                        EntityManager.add(expReplica);
                    }
                    //and fall through to next clause
                }
                //add replica if appropriate
                if (reportedState == ReplicaState.FINALIZED) {
                    addStoredBlockImmediate(storedBlock, node);
                }

                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                Block b = (Block) getParams()[0];
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.READ).
                        addBlock(LockType.WRITE, b.getBlockId()).
                        addReplica(LockType.WRITE).
                        addCorrupt(LockType.WRITE).
                        addExcess(LockType.WRITE).
                        addReplicaUc(LockType.WRITE).
                        addUnderReplicatedBlock(LockType.WRITE).
                        addInvalidatedBlock(LockType.READ).
                        addPendingBlock(LockType.READ);
                lm.acquireByBlock();
            }
        };

        while (itBR.hasNext()) {
            Block iblk = itBR.next();
            ReplicaState reportedState = itBR.getCurrentReplicaState();
            processFirstBlockReportHandler.setParams(iblk, reportedState);
            processFirstBlockReportHandler.handle();
        }
    }

    /**
     * Process a block replica reported by the data-node. No side effects except
     * adding to the passed-in Collections.
     *
     * <ol> <li>If the block is not known to the system (not in blocksMap) then
     * the data-node should be notified to invalidate this block.</li> <li>If
     * the reported replica is valid that is has the same generation stamp and
     * length as recorded on the name-node, then the replica location should be
     * added to the name-node.</li> <li>If the reported replica is not valid,
     * then it is marked as corrupt, which triggers replication of the existing
     * valid replicas. Corrupt replicas are removed from the system when the
     * block is fully replicated.</li> <li>If the reported replica is for a
     * block currently marked "under construction" in the NN, then it should be
     * added to the BlockInfoUnderConstruction's list of replicas.</li> </ol>
     *
     * @param dn descriptor for the datanode that made the report
     * @param block reported block replica
     * @param reportedState reported replica state
     * @param toAdd add to DatanodeDescriptor
     * @param toInvalidate missing blocks (not in the blocks map) should be
     * removed from the data-node
     * @param toCorrupt replicas with unexpected length or generation stamp; add
     * to corrupt replicas
     * @param toUC replicas of blocks currently under construction
     * @return
     * @throws IOException
     */
    private BlockInfo processReportedBlock(final DatanodeDescriptor dn,
            final Block block, final ReplicaState reportedState) throws IOException, PersistanceException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Reported block " + block
                    + " on " + dn.getName() + " size " + block.getNumBytes()
                    + " replicaState = " + reportedState);
        }

        // find block by blockId
        BlockInfo storedBlock = getStoredBlock(block);
        if (storedBlock == null) {
            // If blocksMap does not contain reported block id,
            // the replica should be removed from the data-node.
            NameNode.stateChangeLog.info("BLOCK* processReport: block "
                    + block + " on " + dn.getName() + " size " + block.getNumBytes()
                    + " does not belong to any file.");
            addToInvalidates(block, dn);
            return null;
        }
        BlockUCState ucState = storedBlock.getBlockUCState();

        // Ignore replicas already scheduled to be removed from the DN
        if (invalidateBlocks.contains(dn.getStorageID(), block)) {
            assert !storedBlock.hasReplicaIn(dn.getStorageID()) : "Block " + block
                    + " in recentInvalidatesSet should not appear in DN " + dn;
            return storedBlock;
        }

        if (isReplicaCorrupt(block, reportedState, storedBlock, ucState, dn)) {
            markBlockAsCorrupt(storedBlock, dn);
            return storedBlock;
        }

        if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
            addStoredBlockUnderConstruction((BlockInfoUnderConstruction) storedBlock, dn, reportedState);
            return storedBlock;
        }

        //add replica if appropriate
        if (reportedState == ReplicaState.FINALIZED
                && !storedBlock.hasReplicaIn(dn.getStorageID())) {
            addStoredBlock(storedBlock, dn, null, true);
        }
        return storedBlock;
    }

    /**
     * Process a block replica reported by the data-node. No side effects except
     * adding to the passed-in Collections.
     *
     * <ol> <li>If the block is not known to the system (not in blocksMap) then
     * the data-node should be notified to invalidate this block.</li> <li>If
     * the reported replica is valid that is has the same generation stamp and
     * length as recorded on the name-node, then the replica location should be
     * added to the name-node.</li> <li>If the reported replica is not valid,
     * then it is marked as corrupt, which triggers replication of the existing
     * valid replicas. Corrupt replicas are removed from the system when the
     * block is fully replicated.</li> <li>If the reported replica is for a
     * block currently marked "under construction" in the NN, then it should be
     * added to the BlockInfoUnderConstruction's list of replicas.</li> </ol>
     *
     * @param dn descriptor for the datanode that made the report
     * @param block reported block replica
     * @param reportedState reported replica state
     * @param toAdd add to DatanodeDescriptor
     * @param toInvalidate missing blocks (not in the blocks map) should be
     * removed from the data-node
     * @param toCorrupt replicas with unexpected length or generation stamp; add
     * to corrupt replicas
     * @param toUC replicas of blocks currently under construction
     * @return
     * @throws IOException
     */
    private BlockInfo processReportedBlock(final DatanodeDescriptor dn,
            final Block block, final ReplicaState reportedState,
            final Collection<BlockInfo> toAdd,
            final Collection<Block> toInvalidate,
            final Collection<BlockInfo> toCorrupt,
            final Collection<StatefulBlockInfo> toUC) throws IOException, PersistanceException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Reported block " + block
                    + " on " + dn.getName() + " size " + block.getNumBytes()
                    + " replicaState = " + reportedState);
        }

        // find block by blockId
        BlockInfo storedBlock = getStoredBlock(block);
        if (storedBlock == null) {
            // If blocksMap does not contain reported block id,
            // the replica should be removed from the data-node.
            toInvalidate.add(new Block(block));
            return null;
        }
        BlockUCState ucState = storedBlock.getBlockUCState();

        // Ignore replicas already scheduled to be removed from the DN
        if (invalidateBlocks.contains(dn.getStorageID(), block)) {
            assert !storedBlock.hasReplicaIn(dn.getStorageID()) : "Block " + block
                    + " in recentInvalidatesSet should not appear in DN " + dn;
            return storedBlock;
        }

        if (isReplicaCorrupt(block, reportedState, storedBlock, ucState, dn)) {
            toCorrupt.add(storedBlock);
            return storedBlock;
        }

        if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
            toUC.add(new StatefulBlockInfo(
                    (BlockInfoUnderConstruction) storedBlock, reportedState));
            return storedBlock;
        }

        //add replica if appropriate
        if (reportedState == ReplicaState.FINALIZED
                && !storedBlock.hasReplicaIn(dn.getStorageID())) {
            toAdd.add(storedBlock);
        }
        return storedBlock;
    }

    /*
     * The next two methods test the various cases under which we must conclude
     * the replica is corrupt, or under construction. These are laid out as switch
     * statements, on the theory that it is easier to understand the combinatorics
     * of reportedState and ucState that way. It should be at least as efficient
     * as boolean expressions.
     */
    private boolean isReplicaCorrupt(Block iblk, ReplicaState reportedState,
            BlockInfo storedBlock, BlockUCState ucState,
            DatanodeDescriptor dn) {
        switch (reportedState) {
            case FINALIZED:
                switch (ucState) {
                    case COMPLETE:
                    case COMMITTED:
                        return (storedBlock.getGenerationStamp() != iblk.getGenerationStamp()
                                || storedBlock.getNumBytes() != iblk.getNumBytes());
                    default:
                        return false;
                }
            case RBW:
            case RWR:
                return storedBlock.isComplete();
            case RUR:       // should not be reported
            case TEMPORARY: // should not be reported
            default:
                LOG.warn("Unexpected replica state " + reportedState
                        + " for block: " + storedBlock
                        + " on " + dn.getName() + " size " + storedBlock.getNumBytes());
                return true;
        }
    }

    private boolean isBlockUnderConstruction(BlockInfo storedBlock,
            BlockUCState ucState, ReplicaState reportedState) {
        switch (reportedState) {
            case FINALIZED:
                switch (ucState) {
                    case UNDER_CONSTRUCTION:
                    case UNDER_RECOVERY:
                        return true;
                    default:
                        return false;
                }
            case RBW:
            case RWR:
                return (!storedBlock.isComplete());
            case RUR:       // should not be reported                                                                                             
            case TEMPORARY: // should not be reported                                                                                             
            default:
                return false;
        }
    }

    void addStoredBlockUnderConstruction(
            BlockInfoUnderConstruction block,
            DatanodeDescriptor node,
            ReplicaState reportedState)
            throws IOException, PersistanceException {
        ReplicaUnderConstruction expReplica = block.addExpectedReplica(node.getStorageID(), reportedState);
        if (expReplica != null) {
            EntityManager.add(expReplica);
        }
        if (reportedState == ReplicaState.FINALIZED && !block.hasReplicaIn(node.getStorageID())) {
            addStoredBlock(block, node, null, true);
        }
    }

    /**
     * Faster version of {@link addStoredBlock()}, intended for use with initial
     * block report at startup. If not in startup safe mode, will call standard
     * addStoredBlock(). Assumes this method is called "immediately" so there is
     * no need to refresh the storedBlock from blocksMap. Doesn't handle
     * underReplication/overReplication, or worry about pendingReplications or
     * corruptReplicas, because it's in startup safe mode. Doesn't log every
     * block, because there are typically millions of them.
     *
     * @throws IOException
     */
    private void addStoredBlockImmediate(BlockInfo storedBlock,
            DatanodeDescriptor node)
            throws IOException, PersistanceException {

        assert (storedBlock != null && namesystem.hasWriteLock());
        if (!namesystem.isInStartupSafeMode()
                || namesystem.isPopulatingReplQueues()) {
            addStoredBlock(storedBlock, node, null, false);
            return;
        }

        // just add it
        IndexedReplica replica = storedBlock.addReplica(node);
        if (replica != null) {
            EntityManager.add(replica);
        }

        // Now check for completion of blocks and safe block count
        int numCurrentReplica = countLiveNodes(storedBlock);
        if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
                && numCurrentReplica >= minReplication) {
            storedBlock = completeBlock(storedBlock.getINode(), storedBlock);
        }

        // check whether safe replication is reached for the block
        // only complete blocks are counted towards that
        if (storedBlock.isComplete()) {
            namesystem.incrementSafeBlockCount(numCurrentReplica);
        }
    }

    /**
     * Modify (block-->datanode) map. Remove block from set of needed
     * replications if this takes care of the problem.
     *
     * @return the block that is stored in blockMap.
     */
    private Block addStoredBlock(final BlockInfo block,
            DatanodeDescriptor node,
            DatanodeDescriptor delNodeHint,
            boolean logEveryBlock)
            throws IOException, PersistanceException {
        assert block != null && namesystem.hasWriteLock();
        BlockInfo storedBlock;
        if (block instanceof BlockInfoUnderConstruction) {
            //refresh our copy in case the block got completed in another thread
            storedBlock = getStoredBlock(block);
        } else {
            storedBlock = block;
        }
        if (storedBlock == null || storedBlock.getINode() == null) {
            // If this block does not belong to anyfile, then we are done.
            NameNode.stateChangeLog.info("BLOCK* addStoredBlock: " + block + " on "
                    + node.getName() + " size " + block.getNumBytes()
                    + " but it does not belong to any file.");
            // we could add this block to invalidate set of this datanode.
            // it will happen in next block report otherwise.
            return block;
        }

        assert storedBlock != null : "Block must be stored by now";
        INodeFile fileINode = storedBlock.getINode();
        assert fileINode != null : "Block must belong to a file";
        // add block to the datanode
        IndexedReplica replica = storedBlock.addReplica(node);

        int curReplicaDelta;
        if (replica != null) {
            EntityManager.add(replica);
            curReplicaDelta = 1;
            if (logEveryBlock) {
                NameNode.stateChangeLog.info("BLOCK* addStoredBlock: "
                        + "blockMap updated: " + node.getName() + " is added to "
                        + storedBlock + " size " + storedBlock.getNumBytes());
            }
        } else {
            curReplicaDelta = 0;
            NameNode.stateChangeLog.warn("BLOCK* addStoredBlock: "
                    + "Redundant addStoredBlock request received for " + storedBlock
                    + " on " + node.getName() + " size " + storedBlock.getNumBytes());
        }

        // Now check for completion of blocks and safe block count
        NumberReplicas num = countNodes(storedBlock);
        int numLiveReplicas = num.liveReplicas();
        int numCurrentReplica = numLiveReplicas + curReplicaDelta
                + pendingReplications.getNumReplicas(storedBlock);


        if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
                && numLiveReplicas >= minReplication) {
            storedBlock = completeBlock(fileINode, storedBlock);
        }

        // check whether safe replication is reached for the block
        // only complete blocks are counted towards that
        // Is no-op if not in safe mode.
        if (storedBlock.isComplete()) {
            namesystem.incrementSafeBlockCount(numCurrentReplica);
        }

        // if file is under construction, then done for now
        if (fileINode.isUnderConstruction()) {
            return storedBlock;
        }

        // do not try to handle over/under-replicated blocks during safe mode
        if (!namesystem.isPopulatingReplQueues()) {
            return storedBlock;
        }

        // handle underReplication/overReplication
        short fileReplication = fileINode.getReplication();
        if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
            neededReplications.remove(storedBlock, numCurrentReplica,
                    num.decommissionedReplicas(), fileReplication);
        } else {
            updateNeededReplications(storedBlock, curReplicaDelta, 0);
        }
        if (numCurrentReplica > fileReplication) {
            processOverReplicatedBlock(storedBlock, fileReplication, node, delNodeHint);
        }
        // If the file replication has reached desired value
        // we can remove any corrupt replicas the block may have
        int corruptReplicasCount = numCorruptReplicas(storedBlock);
        int numCorruptNodes = num.corruptReplicas();
        if (numCorruptNodes != corruptReplicasCount) {
            LOG.warn("Inconsistent number of corrupt replicas for "
                    + storedBlock + "blockMap has " + numCorruptNodes
                    + " but corrupt replicas map has " + corruptReplicasCount);
        }
        if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
            invalidateCorruptReplicas(storedBlock);
        }
        return storedBlock;
    }

    /**
     * Invalidate corrupt replicas. <p> This will remove the replicas from the
     * block's location list, add them to {@link #recentInvalidateSets} so that
     * they could be further deleted from the respective data-nodes, and remove
     * the block from corruptReplicasMap. <p> This method should be called when
     * the block has sufficient number of live replicas.
     *
     * @param blk Block whose corrupt replicas need to be invalidated
     */
    private void invalidateCorruptReplicas(Block blk) throws PersistanceException {
        Collection<CorruptReplica> crs = EntityManager.findList(CorruptReplica.Finder.ByBlockId, blk.getBlockId());
        boolean gotException = false;
        if (crs.isEmpty()) {
            return;
        }
        // make a copy of the array of nodes in order to avoid
        // ConcurrentModificationException, when the block is removed from the node
        List<CorruptReplica> list = new ArrayList<CorruptReplica>(crs);
        for (CorruptReplica corruptReplica : list) {
            try {
                invalidateBlock(blk, datanodeManager.getDatanodeByStorageId(corruptReplica.getStorageId()));
            } catch (IOException e) {
                NameNode.stateChangeLog.info("NameNode.invalidateCorruptReplicas "
                        + "error in deleting bad block " + blk
                        + " on " + corruptReplica.getStorageId() + e);
                gotException = true;
            }
        }
        // Remove the block from corruptReplicasMap
        if (!gotException) {
            crs = EntityManager.findList(CorruptReplica.Finder.ByBlockId, blk.getBlockId());
            for (CorruptReplica r : crs) {
                EntityManager.remove(r);
            }
        }
    }

    /**
     * For each block in the name-node verify whether it belongs to any file,
     * over or under replicated. Place it into the respective queue.
     *
     * @throws IOException
     */
    public void processMisReplicatedBlocks() throws IOException, PersistanceException {
        assert namesystem.hasWriteLock();

        long nrInvalid = 0, nrOverReplicated = 0, nrUnderReplicated = 0;
        neededReplications.clear();
        // TODO JIM - if a namenode recovers or starts, it will execute this code.
        // TODO JIM - if the blocks are already validated and the cluster is 'working'
        // we can skip this code. That is, check the leader table for a single working
        // nameNode and if yes, skip the code.
        // But think carefully about how to implement this.
        // Ideally recovering nodes would each choose a range of blocks and process them
        // in parallel.
        for (BlockInfo block : EntityManager.findList(BlockInfo.Finder.All)) {
            INodeFile fileINode = block.getINode();
            if (fileINode == null) {
                // block does not belong to any file
                nrInvalid++;
                addToInvalidates(block);
                continue;
            }
            // calculate current replication
            short expectedReplication = fileINode.getReplication();
            NumberReplicas num = countNodes(block);
            int numCurrentReplica = num.liveReplicas();
            // add to under-replicated queue if need to be
            if (isNeededReplication(block, expectedReplication, numCurrentReplica)) {
                if (neededReplications.add(block, numCurrentReplica, num.decommissionedReplicas(), expectedReplication)) {
                    nrUnderReplicated++;
                }
            }

            if (numCurrentReplica > expectedReplication) {
                // over-replicated block
                nrOverReplicated++;
                processOverReplicatedBlock(block, expectedReplication, null, null);
            }
        }

//    LOG.info("Total number of blocks            = " + blocksMap.size());
        LOG.info("Number of invalid blocks          = " + nrInvalid);
        LOG.info("Number of under-replicated blocks = " + nrUnderReplicated);
        LOG.info("Number of  over-replicated blocks = " + nrOverReplicated);
    }

    /**
     * Set replication for the blocks.
     */
    public void setReplication(final short oldRepl, final short newRepl,
            final String src, List<BlockInfo> blocks) throws IOException, PersistanceException {
        if (newRepl == oldRepl) {
            return;
        }

        // update needReplication priority queues
        for (Block b : blocks) {
            updateNeededReplications(b, 0, newRepl - oldRepl);
        }

        if (oldRepl > newRepl) {
            // old replication > the new one; need to remove copies
            LOG.info("Decreasing replication from " + oldRepl + " to " + newRepl
                    + " for " + src);
            for (Block b : blocks) {
                processOverReplicatedBlock(b, newRepl, null, null);
            }
        } else { // replication factor is increased
            LOG.info("Increasing replication from " + oldRepl + " to " + newRepl
                    + " for " + src);
        }
    }

    /**
     * Find how many of the containing nodes are "extra", if any. If there are
     * any extras, call chooseExcessReplicates() to mark them in the
     * excessReplicateMap.
     *
     * @throws IOException
     */
    private void processOverReplicatedBlock(final Block block,
            final short replication, final DatanodeDescriptor addedNode,
            DatanodeDescriptor delNodeHint) throws IOException, PersistanceException {
        assert namesystem.hasWriteLock();
        if (addedNode == delNodeHint) {
            delNodeHint = null;
        }
        Collection<DatanodeDescriptor> nonExcess = new ArrayList<DatanodeDescriptor>();
        List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(block));

        Collection<ExcessReplica> excessBlocks =
                EntityManager.findList(ExcessReplica.Finder.ByBlockId, block.getBlockId());
        Collection<CorruptReplica> corruptReplicas = EntityManager.findList(CorruptReplica.Finder.ByBlockId, block.getBlockId());
        for (DatanodeDescriptor cur : dataNodes) {
            ExcessReplica searchkey = new ExcessReplica(cur.getStorageID(), block.getBlockId());
            if (excessBlocks == null || !excessBlocks.contains(searchkey)) {
                if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
                    // exclude corrupt replicas
                    if (!corruptReplicas.contains(new CorruptReplica(block.getBlockId(), cur.getStorageID()))) {
                        nonExcess.add(cur);
                    }
                }
            }
        }
        chooseExcessReplicates(nonExcess, block, replication,
                addedNode, delNodeHint, blockplacement);
    }

    /**
     * We want "replication" replicates for the block, but we now have too many.
     * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such
     * that:
     *
     * srcNodes.size() - dstNodes.size() == replication
     *
     * We pick node that make sure that replicas are spread across racks and
     * also try hard to pick one with least free space. The algorithm is first
     * to pick a node with least free space from nodes that are on a rack
     * holding more than one replicas of the block. So removing such a replica
     * won't remove a rack. If no such a node is available, then pick a node
     * with least free space
     *
     * @throws IOException
     */
    private void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess,
            Block b, short replication,
            DatanodeDescriptor addedNode,
            DatanodeDescriptor delNodeHint,
            BlockPlacementPolicy replicator) throws IOException, PersistanceException {
        assert namesystem.hasWriteLock();
        // first form a rack to datanodes map and
        INodeFile inode = getINode(b);
        final Map<String, List<DatanodeDescriptor>> rackMap = new HashMap<String, List<DatanodeDescriptor>>();
        for (final Iterator<DatanodeDescriptor> iter = nonExcess.iterator();
                iter.hasNext();) {
            final DatanodeDescriptor node = iter.next();
            final String rackName = node.getNetworkLocation();
            List<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
            if (datanodeList == null) {
                datanodeList = new ArrayList<DatanodeDescriptor>();
                rackMap.put(rackName, datanodeList);
            }
            datanodeList.add(node);
        }

        // split nodes into two sets
        // priSet contains nodes on rack with more than one replica
        // remains contains the remaining nodes
        final List<DatanodeDescriptor> priSet = new ArrayList<DatanodeDescriptor>();
        final List<DatanodeDescriptor> remains = new ArrayList<DatanodeDescriptor>();
        for (List<DatanodeDescriptor> datanodeList : rackMap.values()) {
            if (datanodeList.size() == 1) {
                remains.add(datanodeList.get(0));
            } else {
                priSet.addAll(datanodeList);
            }
        }

        // pick one node to delete that favors the delete hint
        // otherwise pick one with least space from priSet if it is not empty
        // otherwise one node with least space from remains
        boolean firstOne = true;
        while (nonExcess.size() - replication > 0) {
            // check if we can delete delNodeHint
            final DatanodeInfo cur;
            if (firstOne && delNodeHint != null && nonExcess.contains(delNodeHint)
                    && (priSet.contains(delNodeHint)
                    || (addedNode != null && !priSet.contains(addedNode)))) {
                cur = delNodeHint;
            } else { // regular excessive replica removal
                cur = replicator.chooseReplicaToDelete(inode, b, replication,
                        priSet, remains);
            }
            firstOne = false;

            // adjust rackmap, priSet, and remains
            String rack = cur.getNetworkLocation();
            final List<DatanodeDescriptor> datanodes = rackMap.get(rack);
            datanodes.remove(cur);
            if (datanodes.isEmpty()) {
                rackMap.remove(rack);
            }
            if (priSet.remove(cur)) {
                if (datanodes.size() == 1) {
                    priSet.remove(datanodes.get(0));
                    remains.add(datanodes.get(0));
                }
            } else {
                remains.remove(cur);
            }

            nonExcess.remove(cur);
            addToExcessReplicate(cur, b);

            //
            // The 'excessblocks' tracks blocks until we get confirmation
            // that the datanode has deleted them; the only way we remove them
            // is when we get a "removeBlock" message.  
            //
            // The 'invalidate' list is used to inform the datanode the block 
            // should be deleted.  Items are removed from the invalidate list
            // upon giving instructions to the namenode.
            //
            addToInvalidates(b, cur);
            NameNode.stateChangeLog.info("BLOCK* chooseExcessReplicates: "
                    + "(" + cur.getName() + ", " + b + ") is added to recentInvalidateSets");
        }
    }

    private void addToExcessReplicate(DatanodeInfo dn, Block block) throws PersistanceException {
        assert namesystem.hasWriteLock();

        EntityManager.add(new ExcessReplica(dn.getStorageID(), block.getBlockId()));
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("BLOCK* addToExcessReplicate:"
                    + " (" + dn.getName() + ", " + block
                    + ") is added to excessReplicateMap");
        }
    }

    /**
     * Modify (block-->datanode) map. Possibly generate replication tasks, if
     * the removed block is still valid.
     *
     * @throws IOException
     */
    public void removeStoredBlock(Block block, DatanodeDescriptor node) throws IOException, PersistanceException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("BLOCK* removeStoredBlock: "
                    + block + " from " + node.getName());
        }
        assert (namesystem.hasWriteLock());
        {
            if (!removeNode(block, node)) {
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug("BLOCK* removeStoredBlock: "
                            + block + " has already been removed from node " + node);
                }
                return;
            }

            //
            // It's possible that the block was removed because of a datanode
            // failure. If the block is still valid, check if replication is
            // necessary. In that case, put block on a possibly-will-
            // be-replicated list.
            //
            INode fileINode = getINode(block);
            if (fileINode != null) {
                namesystem.decrementSafeBlockCount(block);
                updateNeededReplications(block, -1, 0);

            }

            //
            // We've removed a block from a node, so it's definitely no longer
            // in "excess" there.
            //
            ExcessReplica exReplica =
                    EntityManager.find(ExcessReplica.Finder.ByPKey, block.getBlockId(), node.getStorageID());
            if (exReplica != null) {
                EntityManager.remove(exReplica);
            }

            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("BLOCK* removeStoredBlock: "
                        + block + " is removed from excessBlocks");
            }
            // Remove the replica from corruptReplicas
            CorruptReplica cr = EntityManager.find(CorruptReplica.Finder.ByPk, block.getBlockId(), node.getStorageID());
            if (cr != null) {
                EntityManager.remove(cr);
            }
        }
    }

    /**
     * Remove data-node reference from the block. Remove the block from the
     * block map only if it does not belong to any file and data-nodes.
     *
     * @throws IOException
     */
    boolean removeNode(Block b, DatanodeDescriptor node) throws IOException, PersistanceException {
        BlockInfo info = getStoredBlock(b);
        if (info == null) {
            return false;
        }
        // remove block from the data-node list and the node from the block info
        IndexedReplica removedReplica = info.removeReplica(node);
        if (removedReplica != null) {
            EntityManager.remove(removedReplica);
        }

        if (info.getReplicas().isEmpty() // no datanodes left
                && info.getINode() == null) {
            try {
                // [H] This condition looks impossible in KTHFS. Cause whenever the inode is deleted the block-info also will be deleted.
                assert false : "This is assumed not to happen. So fix this.";
                // does not belong to a file
                info.getINode().removeBlock(info);
                info.setINode(null);
                EntityManager.remove(info);

            } catch (Exception ex) {
                Logger.getLogger(BlockManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return (removedReplica != null);
    }

    /**
     * Get all valid locations of the block & add the block to results return
     * the length of the added block; 0 if the block is not added
     *
     * @throws IOException
     */
    private long addBlock(Block block, List<BlockWithLocations> results) throws IOException, PersistanceException {
        final List<String> machineSet = getValidLocations(block);
        if (machineSet.size() == 0) {
            return 0;
        } else {
            results.add(new BlockWithLocations(block,
                    machineSet.toArray(new String[machineSet.size()])));
            return block.getNumBytes();
        }
    }

    /**
     * The given node is reporting that it received a certain block.
     */
    @VisibleForTesting
    void addBlock(DatanodeDescriptor node, Block block, String delHint)
            throws IOException, PersistanceException {
        // decrement number of blocks scheduled to this datanode.
        node.decBlocksScheduled();

        // get the deletion hint node
        DatanodeDescriptor delHintNode = null;
        if (delHint != null && delHint.length() != 0) {
            delHintNode = datanodeManager.getDatanodeByStorageId(delHint);
            if (delHintNode == null) {
                NameNode.stateChangeLog.warn("BLOCK* blockReceived: " + block
                        + " is expected to be removed from an unrecorded node " + delHint);
            }
        }

        //
        // Modify the blocks->datanode map and node's map.
        //
        pendingReplications.remove(block);

        // blockReceived reports a finalized block
        Collection<BlockInfo> toAdd = new LinkedList<BlockInfo>();
        Collection<Block> toInvalidate = new LinkedList<Block>();
        Collection<BlockInfo> toCorrupt = new LinkedList<BlockInfo>();
        Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
        processReportedBlock(node, block, ReplicaState.FINALIZED,
                toAdd, toInvalidate, toCorrupt, toUC);
        // the block is only in one of the to-do lists
        // if it is in none then data-node already has it
        assert toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <= 1 : "The block should be only in one of the lists.";

        for (StatefulBlockInfo b : toUC) {
            addStoredBlockUnderConstruction(b.storedBlock, node, b.reportedState);
        }
        for (BlockInfo b : toAdd) {
            addStoredBlock(b, node, delHintNode, true);
        }
        for (Block b : toInvalidate) {
            NameNode.stateChangeLog.info("BLOCK* addBlock: block "
                    + b + " on " + node.getName() + " size " + b.getNumBytes()
                    + " does not belong to any file.");
            addToInvalidates(b, node);
        }
        for (BlockInfo b : toCorrupt) {
            markBlockAsCorrupt(b, node);
        }
    }

    /**
     * The given node is reporting that it received/deleted certain blocks.
     */
    public void blockReceivedAndDeleted(final DatanodeID nodeID,
            final String poolId,
            final ReceivedDeletedBlockInfo receivedAndDeletedBlocks[]) throws IOException {
        namesystem.writeLock();
        try {
            final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
            if (node == null || !node.isAlive) {
                NameNode.stateChangeLog.warn("BLOCK* blockReceivedDeleted"
                        + " is received from dead or unregistered node "
                        + nodeID.getName());
                throw new IOException(
                        "Got blockReceivedDeleted message from unregistered or dead node");
            }

            TransactionalRequestHandler blockReceivedAndDeletedHandler = new TransactionalRequestHandler(OperationType.BLOCK_RECEIVED_AND_DELETED) {
                @Override
                public Object performTask() throws PersistanceException, IOException {
                    ReceivedDeletedBlockInfo receivedAndDeletedBlock = (ReceivedDeletedBlockInfo) getParams()[0];

                    if (receivedAndDeletedBlock.isDeletedBlock()) {
                        removeStoredBlock(
                                receivedAndDeletedBlock.getBlock(), node);
                    } else {
                        addBlock(node, receivedAndDeletedBlock.getBlock(),
                                receivedAndDeletedBlock.getDelHints());
                    }
                    return null;
                }

                @Override
                public void acquireLock() throws PersistanceException, IOException {
                    ReceivedDeletedBlockInfo receivedAndDeletedBlock = (ReceivedDeletedBlockInfo) getParams()[0];
                    TransactionLockManager lm = new TransactionLockManager();
                    lm.addINode(TransactionLockManager.INodeLockType.WRITE).
                            addBlock(LockType.WRITE, receivedAndDeletedBlock.getBlock().getBlockId()).
                            addReplica(LockType.WRITE).
                            addExcess(LockType.WRITE).
                            addCorrupt(LockType.WRITE).
                            addUnderReplicatedBlock(LockType.WRITE);
                    if (!receivedAndDeletedBlock.isDeletedBlock()) {
                        lm.addPendingBlock(LockType.WRITE).
                                addReplicaUc(LockType.WRITE).
                                addInvalidatedBlock(LockType.READ);
                    }
                    lm.acquireByBlock();
                }
            };

            for (int i = 0; i < receivedAndDeletedBlocks.length; i++) {
                blockReceivedAndDeletedHandler.setParams(receivedAndDeletedBlocks[i]);
                blockReceivedAndDeletedHandler.handle();
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug("BLOCK* block"
                            + (receivedAndDeletedBlocks[i].isDeletedBlock() ? "Deleted"
                            : "Received") + ": " + receivedAndDeletedBlocks[i].getBlock()
                            + " is received from " + nodeID.getName());
                }
            }
        } finally {
            namesystem.writeUnlock();
        }
    }

    /**
     * Return the number of nodes that are live and decommissioned.
     */
    public NumberReplicas countNodes(Block b, DatanodeDescriptor dnDescriptors[]) throws PersistanceException {
        int count = 0;
        int live = 0;
        int corrupt = 0;
        int excess = 0;


        //Iterator<DatanodeDescriptor> nodeIter = blocksMap.nodeIterator(b);
        List<DatanodeDescriptor> liveDescriptors = new ArrayList<DatanodeDescriptor>();
        //while (nodeIter.hasNext()) {
        for (int i = 0; i < dnDescriptors.length; i++) {
            //DatanodeDescriptor node = nodeIter.next();
            DatanodeDescriptor node = dnDescriptors[i];
            if (isItCorruptedReplica(b.getBlockId(), node.getStorageID())) {
                corrupt++;
            } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
                count++;
            } else {
                Collection<ExcessReplica> blocksExcess =
                        EntityManager.findList(ExcessReplica.Finder.ByStorageId, node.getStorageID());
                ExcessReplica searchKey = new ExcessReplica(node.getStorageID(), b.getBlockId());
                if (blocksExcess != null && blocksExcess.contains(searchKey)) {
                    excess++;
                } else {
                    live++;
                    liveDescriptors.add(node);
                }
            }
        }
        return new NumberReplicas(live, count, corrupt, excess);
    }

    /**
     * Return the number of nodes that are live and decommissioned.
     */
    public NumberReplicas countNodes(Block b) throws IOException, PersistanceException {
        int count = 0;
        int live = 0;
        int corrupt = 0;
        int excess = 0;

        List<DatanodeDescriptor> dnDescriptors = getDatanodes(getStoredBlock(b));
        Collection<CorruptReplica> corrupts = EntityManager.findList(CorruptReplica.Finder.ByBlockId, b.getBlockId());

        for (DatanodeDescriptor node : dnDescriptors) {
            if (corrupts.contains(new CorruptReplica(b.getBlockId(), node.getStorageID()))) {
                corrupt++;
            } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
                count++;
            } else {
                Collection<ExcessReplica> blocksExcess =
                        EntityManager.findList(ExcessReplica.Finder.ByBlockId, b.getBlockId());
                ExcessReplica searchKey = new ExcessReplica(node.getStorageID(), b.getBlockId());
                if (blocksExcess != null && blocksExcess.contains(searchKey)) {
                    excess++;
                } else {
                    live++;
                }
            }

        }

        return new NumberReplicas(live, count, corrupt, excess);
    }

    /**
     * KTHFS Change: Count live nodes for a block via NDB from replicas table
     *
     * Simpler, faster form of {@link countNodes()} that only returns the number
     * of live nodes. If in startup safemode (or its 30-sec extension period),
     * then it gains speed by ignoring issues of excess replicas or nodes that
     * are decommissioned or in process of becoming decommissioned. If not in
     * startup, then it calls {@link countNodes()} instead.
     *
     * Original HDFS version: {@link countLiveNodesOld()}
     *
     * @param b - the block being tested
     * @return count of live nodes for this block
     */
    int countLiveNodes(BlockInfo b) throws IOException, PersistanceException {
        if (!namesystem.isInStartupSafeMode()) {
            return countNodes(b).liveReplicas();
        }
        // else proceed with fast case
        int live = 0;
        List<DatanodeDescriptor> dnDescriptors = getDatanodes(b);

        for (DatanodeDescriptor node : dnDescriptors) {
            if (!isItCorruptedReplica(b.getBlockId(), node.getStorageID())) {
                live++;
            }

        }

        // FIXME: JIM When datanode restarts, it is assigned with another port and identified 
        // as a new replica (which is false) and then its added to replicas 
        // as another duplicate replica
        return live;
    }

    private void logBlockReplicationInfo(Block block, DatanodeDescriptor srcNode,
            NumberReplicas num) throws IOException, PersistanceException {
        int curReplicas = num.liveReplicas();
        int curExpectedReplicas = getReplication(block);
        INode fileINode = getINode(block);

        List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(block));

        StringBuilder nodeList = new StringBuilder();
        for (DatanodeDescriptor node : dataNodes) {
            nodeList.append(node.name);
            nodeList.append(" ");
        }
        LOG.info("Block: " + block + ", Expected Replicas: "
                + curExpectedReplicas + ", live replicas: " + curReplicas
                + ", corrupt replicas: " + num.corruptReplicas()
                + ", decommissioned replicas: " + num.decommissionedReplicas()
                + ", excess replicas: " + num.excessReplicas()
                + ", Is Open File: " + fileINode.isUnderConstruction()
                + ", Datanodes having this block: " + nodeList + ", Current Datanode: "
                + srcNode.name + ", Is current datanode decommissioning: "
                + srcNode.isDecommissionInProgress());
    }

    /**
     * On stopping decommission, check if the node has excess replicas. If there
     * are any excess replicas, call processOverReplicatedBlock()
     *
     * @throws IOException
     */
    void processOverReplicatedBlocksOnReCommission(
            final DatanodeDescriptor srcNode, OperationType opType) throws IOException {

        final Iterator<? extends Block> it = findBlocksAssociatedTo(srcNode.getStorageID(), opType);

        TransactionalRequestHandler processBlockHandler = new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                final Block block = (Block) getParams()[0];
                INodeFile fileINode = getINode(block);
                short expectedReplication = fileINode.getReplication();
                NumberReplicas num = countNodes(block);
                int numCurrentReplica = num.liveReplicas();
                if (numCurrentReplica > expectedReplication) {
                    // over-replicated block 
                    processOverReplicatedBlock(block, expectedReplication, null, null);
                }
                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                final Block block = (Block) getParams()[0];
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.READ).
                        addBlock(LockType.WRITE, block.getBlockId()).
                        addReplica(LockType.READ).
                        addExcess(LockType.READ).
                        addCorrupt(LockType.READ).
                        addPendingBlock(LockType.READ).
                        addUnderReplicatedBlock(LockType.WRITE);
                lm.acquireByBlock();
            }
        };

        while (it.hasNext()) {
            processBlockHandler.setParams(it.next()).handle();
        }
    }

    /**
     * Return true if there are any blocks on this node that have not yet
     * reached their replication factor. Otherwise returns false.
     *
     * @throws IOException
     */
    boolean isReplicationInProgress(final DatanodeDescriptor srcNode, OperationType opType) throws IOException {
        final boolean[] status = new boolean[]{false};
        final int[] underReplicatedBlocks = new int[]{0};
        final int[] decommissionOnlyReplicas = new int[]{0};
        final int[] underReplicatedInOpenFiles = new int[]{0};

        final Iterator<? extends Block> it = findBlocksAssociatedTo(srcNode.getStorageID(), opType);

        TransactionalRequestHandler checkReplicationHandler = new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                final Block block = (Block) getParams()[0];

                INode fileINode = getINode(block);


                if (fileINode != null) {
                    NumberReplicas num = countNodes(block);
                    int curReplicas = num.liveReplicas();
                    int curExpectedReplicas = getReplication(block);
                    if (isNeededReplication(block, curExpectedReplicas, curReplicas)) {
                        if (curExpectedReplicas > curReplicas) {
                            //Log info about one block for this node which needs replication
                            if (!status[0]) {
                                status[0] = true;
                                logBlockReplicationInfo(block, srcNode, num);
                            }
                            underReplicatedBlocks[0]++;
                            if ((curReplicas == 0) && (num.decommissionedReplicas() > 0)) {
                                decommissionOnlyReplicas[0]++;
                            }
                            if (fileINode.isUnderConstruction()) {
                                underReplicatedInOpenFiles[0]++;
                            }
                        }
                        if (!neededReplications.contains(block)
                                && pendingReplications.getNumReplicas(block) == 0) {
                            //
                            // These blocks have been reported from the datanode
                            // after the startDecommission method has been executed. These
                            // blocks were in flight when the decommissioning was started.
                            //
                            neededReplications.add(block,
                                    curReplicas,
                                    num.decommissionedReplicas(),
                                    curExpectedReplicas);
                        }
                    }
                }
                return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                final Block block = (Block) getParams()[0];
                TransactionLockManager lm = new TransactionLockManager();
                lm.addINode(TransactionLockManager.INodeLockType.READ).
                        addBlock(LockType.WRITE, block.getBlockId()).
                        addReplica(LockType.READ).
                        addExcess(LockType.READ).
                        addCorrupt(LockType.READ).
                        addUnderReplicatedBlock(LockType.WRITE).
                        addPendingBlock(LockType.READ);
                lm.acquireByBlock();
            }
        };

        while (it.hasNext()) {
            checkReplicationHandler.setParams(it.next()).handle();
        }
        srcNode.decommissioningStatus.set(underReplicatedBlocks[0],
                decommissionOnlyReplicas[0],
                underReplicatedInOpenFiles[0]);
        return status[0];
    }

    public int getActiveBlockCount(OperationType opType) throws IOException {

        return getTotalBlocksInternal(opType) - invalidateBlocks.numBlocks(opType);
    }

    private int getTotalBlocksInternal(OperationType opType) throws IOException {
        return (Integer) new LightWeightRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                BlockInfoDataAccess da = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
                return da.countAll();
            }
        }.handle();
    }

    public int getTotalBlocks(OperationType opType) throws IOException {
        return getTotalBlocksInternal(opType);
    }

    public void removeBlock(Block block) throws IOException, PersistanceException {
        block.setNumBytes(BlockCommand.NO_ACK);
        addToInvalidates(block);
        removeBlockFromMap(block);
    }

    public BlockInfo getStoredBlock(Block block) throws IOException, PersistanceException {
        return EntityManager.find(BlockInfo.Finder.ById, block.getBlockId());
    }

    /**
     * updates a block in under replication queue
     *
     * @throws IOException
     */
    private void updateNeededReplications(final Block block,
            final int curReplicasDelta, int expectedReplicasDelta) throws IOException, PersistanceException {
        namesystem.writeLock();
        try {
            NumberReplicas repl = countNodes(block);
            int curExpectedReplicas = getReplication(block);
            if (isNeededReplication(block, curExpectedReplicas, repl.liveReplicas())) {
                neededReplications.update(block, repl.liveReplicas(), repl.decommissionedReplicas(), curExpectedReplicas, curReplicasDelta,
                        expectedReplicasDelta);
            } else {
                int oldReplicas = repl.liveReplicas() - curReplicasDelta;
                int oldExpectedReplicas = curExpectedReplicas - expectedReplicasDelta;
                neededReplications.remove(block, oldReplicas, repl.decommissionedReplicas(),
                        oldExpectedReplicas);
            }
        } finally {
            namesystem.writeUnlock();
        }
    }

    public void checkReplication(Block block, int numExpectedReplicas) throws IOException, PersistanceException {
        // filter out containingNodes that are marked for decommission.
        NumberReplicas number = countNodes(block);
        if (isNeededReplication(block, numExpectedReplicas, number.liveReplicas())) {
            neededReplications.add(block,
                    number.liveReplicas(),
                    number.decommissionedReplicas(),
                    numExpectedReplicas);
        }
    }

    /*
     * get replication factor of a block
     */
    private int getReplication(Block block) throws IOException, PersistanceException {
        INodeFile fileINode = getINode(block);

        if (fileINode == null) { // block does not belong to any file
            return 0;
        }
        assert !fileINode.isDirectory() : "Block cannot belong to a directory.";
        return fileINode.getReplication();
    }

    /**
     * Get blocks to invalidate for <i>nodeId</i> in
     * {@link #recentInvalidateSets}.
     *
     * @return number of blocks scheduled for removal during this iteration.
     */
    private int invalidateWorkForOneNode(String nodeId, OperationType opType) throws IOException {
        namesystem.writeLock();
        try {
            // blocks should not be replicated or removed if safe mode is on
            if (isInSafeMode(opType)) {
                return 0;
            }
            // get blocks to invalidate for the nodeId
            assert nodeId != null;
            return invalidateBlocks.invalidateWork(nodeId, opType);
        } finally {
            namesystem.writeUnlock();
        }
    }

    boolean blockHasEnoughRacks(Block b) throws IOException, PersistanceException {
        if (!this.shouldCheckForEnoughRacks) {
            return true;
        }
        boolean enoughRacks = false;
        int numExpectedReplicas = getReplication(b);
        String rackName = null;
        List<DatanodeDescriptor> dataNodes = getDatanodes(getStoredBlock(b));

        for (DatanodeDescriptor cur : dataNodes) {
            if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
                if (!isItCorruptedReplica(b.getBlockId(), cur.getStorageID())) {
                    if (numExpectedReplicas == 1) {
                        enoughRacks = true;
                        break;
                    }
                    String rackNameNew = cur.getNetworkLocation();
                    if (rackName == null) {
                        rackName = rackNameNew;
                    } else if (!rackName.equals(rackNameNew)) {
                        enoughRacks = true;
                        break;
                    }
                }
            }
        }
        return enoughRacks;
    }

    boolean isNeededReplication(Block b, int expectedReplication, int curReplicas) throws IOException, PersistanceException {
        if ((curReplicas >= expectedReplication) && (blockHasEnoughRacks(b))) {
            return false;
        } else {
            return true;
        }
    }

    public long getMissingBlocksCount(OperationType opType) throws IOException {
        // not locking
        return this.neededReplications.getCorruptBlockSize(opType);
    }

    public INodeFile getINode(Block b) throws IOException, PersistanceException {
        if (b instanceof BlockInfo) {
            return ((BlockInfo) b).getINode();
        } else {
            b = EntityManager.find(BlockInfo.Finder.ById, b.getBlockId());
            if (b == null) {
                return null;
            }
            return ((BlockInfo) b).getINode();
        }
    }

    public int numCorruptReplicas(Block block) throws PersistanceException {
        return EntityManager.findList(CorruptReplica.Finder.ByBlockId, block.getBlockId()).size();
    }

    public boolean isItCorruptedReplica(long blockId, String storageId) throws PersistanceException {
        return EntityManager.find(CorruptReplica.Finder.ByPk, blockId, storageId) != null;
    }

    public void removeBlockFromMap(Block block) throws IOException, PersistanceException {

        Collection<CorruptReplica> crs = EntityManager.findList(CorruptReplica.Finder.ByBlockId, block.getBlockId());
        for (CorruptReplica r : crs) {
            EntityManager.remove(r);
        }
        BlockInfo bi = getStoredBlock(block);
        if (bi != null) {
//            INodeFile iNode = bi.getINode();
//            if (iNode != null) {
//                iNode.removeBlock(bi);      //TODO [S] does not remove any thing 
//                                            //it modifies the block's record in db
//                                            //its appeart that this is redundant as the 
//                                            // modified row will be deleted later in this
//                                            // transaction
//            }
//            bi.setINode(null);
//            for (IndexedReplica replica : bi.getReplicas()) {
//                EntityManager.remove(replica);
//            }

            if (bi instanceof BlockInfoUnderConstruction) {
                for (ReplicaUnderConstruction ruc : ((BlockInfoUnderConstruction) bi).getExpectedReplicas()) {
                    EntityManager.remove(ruc);
                }
            }
            EntityManager.remove(bi);

            // Remove all the replicas for this block           // TODO [S] didnt we do this above
            Collection<IndexedReplica> replicas = EntityManager.findList(IndexedReplica.Finder.ByBlockId, block.getBlockId());
            for (IndexedReplica r : replicas) {
                EntityManager.remove(r);
            }
            
            // Remove all the under entries from the under replicated blocks table
            UnderReplicatedBlock urb = EntityManager.find(UnderReplicatedBlock.Finder.ByBlockId, block.getBlockId());
            System.out.println(urb);
            if(urb != null)
            {
                EntityManager.remove(urb);
            }           
        }
    }

    public int getCapacity() {
        namesystem.readLock();
        try {
            return Integer.MAX_VALUE;
        } finally {
            namesystem.readUnlock();
        }
    }

    /**
     * Return a range of corrupt replica block ids. Up to numExpectedBlocks
     * blocks starting at the next block after startingBlockId are returned
     * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId
     * is null, up to numExpectedBlocks blocks are returned from the beginning.
     * If startingBlockId cannot be found, null is returned.
     *
     * @param numExpectedBlocks Number of block ids to return. 0 <=
     * numExpectedBlocks <= 100
     * @param startingBlockId Block id from which to start. If null, start at
     * beginning.
     * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
     *
     */
    public Long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
            Long startingBlockId) throws PersistanceException {
        //this feature is distabled temprarily, it is not essential
        return EntityManager.findList(CorruptReplica.Finder.All).toArray(new Long[0]);
    }

    /**
     * @return the size of UnderReplicatedBlocks
     */
    public int numOfUnderReplicatedBlocks(OperationType opType) {
        try {
            return neededReplications.size(opType);
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            return -1;
        }
    }

    public List<DatanodeDescriptor> getDatanodes(BlockInfo block) throws PersistanceException {
        List<DatanodeDescriptor> nonDecommissionedNodesForTheBlock = new ArrayList<DatanodeDescriptor>();
        List<DatanodeDescriptor> decommissionedNodesForTheBlock = new ArrayList<DatanodeDescriptor>();
        
        for (IndexedReplica replica : block.getReplicas()) {
            DatanodeDescriptor dn = datanodeManager.getDatanodeByStorageId(replica.getStorageId()); 
            if (dn != null) 
            {
                
                if(dn.isDecommissioned())
                {
                    decommissionedNodesForTheBlock.add(dn);
                }
                else
                {    
                    nonDecommissionedNodesForTheBlock.add(dn);
                }
            }
        }

        List<DatanodeDescriptor> allDatanodeDescForTheBlock = new ArrayList<DatanodeDescriptor>();
        allDatanodeDescForTheBlock.addAll(nonDecommissionedNodesForTheBlock);
        allDatanodeDescForTheBlock.addAll(decommissionedNodesForTheBlock);
        return allDatanodeDescForTheBlock;
    }

    public DatanodeDescriptor[] getExpectedDatanodes(BlockInfoUnderConstruction block) throws PersistanceException {
        List<DatanodeDescriptor> list = new ArrayList<DatanodeDescriptor>();

        for (ReplicaUnderConstruction uc : block.getExpectedReplicas()) {
            DatanodeDescriptor dn = datanodeManager.getDatanodeByStorageId(uc.getStorageId());
            if (dn != null) {
                list.add(dn);
            }
        }

        DatanodeDescriptor[] array = new DatanodeDescriptor[list.size()];

        array = list.toArray(array);

        return array;
    }

    /**
     * Periodically calls computeReplicationWork().
     */
    private class ReplicationMonitor implements Runnable {

        private static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
        private static final int REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;

        @Override
        public void run() {
            while (namesystem.isRunning()) {
                try {
                    if (!namesystem.isLeader()) {
                        Thread.sleep(replicationRecheckInterval);
                        continue;
                    }
                } catch (InterruptedException ie) {
                    LOG.warn("ReplicationMonitor thread received InterruptedException.", ie);
                    break;
                }

                try {
                    computeDatanodeWork(OperationType.REPLICATION_MONITOR);
                    processPendingReplications(OperationType.REPLICATION_MONITOR);
                    // TODO - JIM do we want the leader to wait before
                    // rechecking replication state?
                    Thread.sleep(replicationRecheckInterval);
                } catch (InterruptedException ie) {
                    LOG.warn("ReplicationMonitor thread received InterruptedException.", ie);
                    break;
                } catch (IOException ie) {
                    LOG.warn("ReplicationMonitor thread received exception. ", ie);
                } catch (Throwable t) {
                    LOG.warn("ReplicationMonitor thread received Runtime exception. ", t);
                    Runtime.getRuntime().exit(-1);
                }
            }
        }
    }

    /**
     * Compute block replication and block invalidation work that can be
     * scheduled on data-nodes. The datanode will be informed of this work at
     * the next heartbeat.
     *
     * @return number of blocks scheduled for replication or removal.
     * @throws IOException
     */
    int computeDatanodeWork(OperationType opType) throws IOException {
        int workFound = 0;
        // Blocks should not be replicated or removed if in safe mode.
        // It's OK to check safe mode here w/o holding lock, in the worst
        // case extra replications will be scheduled, and these will get
        // fixed up later.
        if (isInSafeMode(opType)) {
            return workFound;
        }

        final int numlive = heartbeatManager.getLiveDatanodeCount();
        final int blocksToProcess = numlive
                * ReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION;
        final int nodesToProcess = (int) Math.ceil(numlive
                * ReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION / 100.0);

        workFound = this.computeReplicationWork(blocksToProcess, opType);

        // Update FSNamesystemMetrics counters
        namesystem.writeLock();
        try {
            updateState(opType);
            this.scheduledReplicationBlocksCount = workFound;
        } finally {
            namesystem.writeUnlock();
        }
        workFound += this.computeInvalidateWork(nodesToProcess, opType);
        return workFound;
    }

    private boolean isInSafeMode(OperationType opType) throws IOException {
        return (Boolean) new TransactionalRequestHandler(opType) {
            @Override
            public Object performTask() throws PersistanceException, IOException {
                return namesystem.isInSafeMode();
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
                // FIXME [H]: What locks should be acquired for this?
            }
        }.handle();
    }
}
