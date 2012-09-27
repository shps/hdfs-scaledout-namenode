package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
  private static Map<Class, EntityDataAccess> dataAccessMap = new HashMap<Class, EntityDataAccess>();

  private static void initDataAccessMap() {
    dataAccessMap.put(blockInfoDataAccess.getClass(), blockInfoDataAccess);
    dataAccessMap.put(corruptReplicaDataAccess.getClass(), corruptReplicaDataAccess);
    dataAccessMap.put(excessReplicaDataAccess.getClass(), excessReplicaDataAccess);
    dataAccessMap.put(inodeDataAccess.getClass(), inodeDataAccess);
    dataAccessMap.put(invalidateBlockDataAccess.getClass(), invalidateBlockDataAccess);
    dataAccessMap.put(leaseDataAccess.getClass(), leaseDataAccess);
    dataAccessMap.put(leasePathDataAccess.getClass(), leasePathDataAccess);
    dataAccessMap.put(pendingBlockDataAccess.getClass(), pendingBlockDataAccess);
    dataAccessMap.put(replicaDataAccess.getClass(), replicaDataAccess);
    dataAccessMap.put(replicaUnderConstruntionDataAccess.getClass(), replicaUnderConstruntionDataAccess);
    dataAccessMap.put(underReplicatedBlockDataAccess.getClass(), underReplicatedBlockDataAccess);
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
    return entityContexts;
  }

  public static EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
}
