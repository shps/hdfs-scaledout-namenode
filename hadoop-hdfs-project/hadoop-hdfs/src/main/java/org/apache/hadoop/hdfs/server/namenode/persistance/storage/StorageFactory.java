package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.*;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageFactory {

  private static StorageConnector defaultStorage;
  private static BlockInfoDataAccess blockInfoDataAccess;
  private static CorruptReplicaDataAccess corruptReplicaDataAccess;
  private static ExcessReplicaDataAccess excessReplicaDataAccess;
  private static InodeDataAccess inodeDataAccess;
  private static InvalidateBlockDataAccess invalidateBlockDataAccess;
  private static LeaseDataAccess leaseDataAccess;
  private static LeasePathDataAccess leasePathDataAccess;
  private static PendingBlockDataAccess pendingBlockDataAccess;
  private static ReplicaDataAccess replicaDataAccess;
  private static ReplicaUnderConstruntionDataAccess replicaUnderConstruntionDataAccess;
  private static UnderReplicatedBlockDataAccess underReplicatedBlockDataAccess;
  private static LeaderDataAccess leaderDataAccess;
  private static BlockTokenKeyDataAccess blockTokenKeyDataAccess;
  private static Map<Class, EntityDataAccess> dataAccessMap = new HashMap<Class, EntityDataAccess>();

  private static void initDataAccessMap() {
    dataAccessMap.put(blockInfoDataAccess.getClass().getSuperclass(), blockInfoDataAccess);
    dataAccessMap.put(corruptReplicaDataAccess.getClass().getSuperclass(), corruptReplicaDataAccess);
    dataAccessMap.put(excessReplicaDataAccess.getClass().getSuperclass(), excessReplicaDataAccess);
    dataAccessMap.put(inodeDataAccess.getClass().getSuperclass(), inodeDataAccess);
    dataAccessMap.put(invalidateBlockDataAccess.getClass().getSuperclass(), invalidateBlockDataAccess);
    dataAccessMap.put(leaseDataAccess.getClass().getSuperclass(), leaseDataAccess);
    dataAccessMap.put(leasePathDataAccess.getClass().getSuperclass(), leasePathDataAccess);
    dataAccessMap.put(pendingBlockDataAccess.getClass().getSuperclass(), pendingBlockDataAccess);
    dataAccessMap.put(replicaDataAccess.getClass().getSuperclass(), replicaDataAccess);
    dataAccessMap.put(replicaUnderConstruntionDataAccess.getClass().getSuperclass(), replicaUnderConstruntionDataAccess);
    dataAccessMap.put(underReplicatedBlockDataAccess.getClass().getSuperclass(), underReplicatedBlockDataAccess);
    dataAccessMap.put(leaderDataAccess.getClass().getSuperclass(), leaderDataAccess);
    dataAccessMap.put(blockTokenKeyDataAccess.getClass().getSuperclass(), blockTokenKeyDataAccess);
  }

  public static StorageConnector getConnector() {
    return defaultStorage;
  }

  public static void setConfiguration(Configuration conf) {
    String storageType = conf.get(DFSConfigKeys.DFS_STORAGE_TYPE_KEY);
    if (storageType.equals(DerbyConnector.DERBY_EMBEDDED)
            || storageType.equals(DerbyConnector.DERBY_NETWORK_SERVER)) {
      defaultStorage = DerbyConnector.INSTANCE;
      defaultStorage.setConfiguration(conf);
      blockInfoDataAccess = new BlockInfoDerby();
      corruptReplicaDataAccess = new CorruptReplicaDerby();
      excessReplicaDataAccess = new ExcessReplicaDerby();
      inodeDataAccess = new InodeDerby();
      invalidateBlockDataAccess = new InvalidatedBlockDerby();
      leaseDataAccess = new LeaseDerby();
      leasePathDataAccess = new LeasePathDerby();
      pendingBlockDataAccess = new PendingBlockDerby();
      replicaDataAccess = new ReplicaDerby();
      replicaUnderConstruntionDataAccess = new ReplicaUnderConstructionDerby();
      underReplicatedBlockDataAccess = new UnderReplicatedBlockDerby();
      leaderDataAccess = new LeaderDerby();
      // TODO[Hooman]: Add derby data access for block token key.
    } else if (storageType.equals("clusterj")) {
      defaultStorage = ClusterjConnector.INSTANCE;
      defaultStorage.setConfiguration(conf);
      blockInfoDataAccess = new BlockInfoClusterj();
      corruptReplicaDataAccess = new CorruptReplicaClusterj();
      excessReplicaDataAccess = new ExcessReplicaClusterj();
      inodeDataAccess = new InodeClusterj();
      invalidateBlockDataAccess = new InvalidatedBlockClusterj();
      leaseDataAccess = new LeaseClusterj();
      leasePathDataAccess = new LeasePathClusterj();
      pendingBlockDataAccess = new PendingBlockClusterj();
      replicaDataAccess = new ReplicaClusterj();
      replicaUnderConstruntionDataAccess = new ReplicaUnderConstructionClusterj();
      underReplicatedBlockDataAccess = new UnderReplicatedBlockClusterj();
      leaderDataAccess = new LeaderClusterj();
      blockTokenKeyDataAccess = new BlockTokenKeyClusterj();
    }

    initDataAccessMap();
  }

  public static Map<Class, EntityContext> createEntityContexts() {
    Map<Class, EntityContext> entityContexts = new HashMap<Class, EntityContext>();
    BlockInfoContext bicj = new BlockInfoContext(blockInfoDataAccess);
    entityContexts.put(BlockInfo.class, bicj);
    entityContexts.put(BlockInfoUnderConstruction.class, bicj);
    entityContexts.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionContext(replicaUnderConstruntionDataAccess));
    entityContexts.put(IndexedReplica.class, new ReplicaContext(replicaDataAccess));
    entityContexts.put(ExcessReplica.class, new ExcessReplicaContext(excessReplicaDataAccess));
    entityContexts.put(InvalidatedBlock.class, new InvalidatedBlockContext(invalidateBlockDataAccess));
    entityContexts.put(Lease.class, new LeaseContext(leaseDataAccess));
    entityContexts.put(LeasePath.class, new LeasePathContext(leasePathDataAccess));
    entityContexts.put(PendingBlockInfo.class, new PendingBlockContext(pendingBlockDataAccess));
    InodeContext inodeContext = new InodeContext(inodeDataAccess);
    entityContexts.put(INode.class, inodeContext);
    entityContexts.put(INodeDirectory.class, inodeContext);
    entityContexts.put(INodeFile.class, inodeContext);
    entityContexts.put(INodeDirectoryWithQuota.class, inodeContext);
    entityContexts.put(INodeSymlink.class, inodeContext);
    entityContexts.put(CorruptReplica.class, new CorruptReplicaContext(corruptReplicaDataAccess));
    entityContexts.put(UnderReplicatedBlock.class, new UnderReplicatedBlockContext(underReplicatedBlockDataAccess));
    entityContexts.put(Leader.class, new LeaderContext(leaderDataAccess));
    entityContexts.put(BlockKey.class, new BlockTokenKeyContext(blockTokenKeyDataAccess));
    return entityContexts;
  }

  public static EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
}
