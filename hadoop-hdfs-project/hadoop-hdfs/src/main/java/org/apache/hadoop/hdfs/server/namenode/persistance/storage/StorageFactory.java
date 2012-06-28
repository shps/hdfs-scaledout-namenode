package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.BlockInfoClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ClusterjConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ExcessReplicaClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.INodeClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.IndexedReplicaClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.InvalidatedBlockClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.LeaseClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.LeasePathClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.PendingBlockClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ReplicaUnderConstructionClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.BlockInfoDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.DerbyConnector;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.ExcessReplicaDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.INodeDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.IndexedReplicaDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.InvalidatedBlockDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.LeaseDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.LeasePathDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.PendingBlockDerby;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.derby.ReplicaUnderConstructionDerby;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageFactory {

  private static final StorageConnector defaultStorage = DerbyConnector.INSTANCE;

  public static StorageConnector getConnector() {
    return defaultStorage;
  }

  public enum StorageType {

    Clusterj, ApacheDerby, Mysql;
  }

  public static Map<Class, Storage> getStorageMap() {
    if (defaultStorage instanceof DerbyConnector) {
      return getStorageMap(StorageType.ApacheDerby);
    } else if (defaultStorage instanceof ClusterjConnector) {
      return getStorageMap(StorageType.Clusterj);
    } else {
      return null;
    }

  }

  private static Map<Class, Storage> getStorageMap(StorageType type) {
    Map<Class, Storage> storageMap = null;

    switch (type) {
      case Clusterj:
        storageMap = getClusterJStorage();
        break;
      case ApacheDerby:
        storageMap = getDerbyStorage();
        break;
      case Mysql:
        throw new UnsupportedOperationException("Mysql is not supported yet.");
    }

    return storageMap;
  }

  private static Map<Class, Storage> getClusterJStorage() {
    Map<Class, Storage> storages = new HashMap<Class, Storage>();
    BlockInfoClusterj bicj = new BlockInfoClusterj();
    storages.put(BlockInfo.class, bicj);
    storages.put(BlockInfoUnderConstruction.class, bicj);
    storages.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionClusterj());
    storages.put(IndexedReplica.class, new IndexedReplicaClusterj());
    storages.put(ExcessReplica.class, new ExcessReplicaClusterj());
    storages.put(InvalidatedBlock.class, new InvalidatedBlockClusterj());
    storages.put(Lease.class, new LeaseClusterj());
    storages.put(LeasePath.class, new LeasePathClusterj());
    storages.put(PendingBlockInfo.class, new PendingBlockClusterj());
    INodeClusterj inodeClusterj = new INodeClusterj();
    storages.put(INode.class, inodeClusterj);
    storages.put(INodeDirectory.class, inodeClusterj);
    storages.put(INodeFile.class, inodeClusterj);
    storages.put(INodeDirectoryWithQuota.class, inodeClusterj);
    storages.put(INodeSymlink.class, inodeClusterj);

    return storages;
  }

  private static Map<Class, Storage> getDerbyStorage() {
    Map<Class, Storage> storages = new HashMap<Class, Storage>();
    BlockInfoDerby bidrby = new BlockInfoDerby();
    storages.put(BlockInfo.class, bidrby);
    storages.put(BlockInfoUnderConstruction.class, bidrby);
    storages.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionDerby());
    storages.put(IndexedReplica.class, new IndexedReplicaDerby());
    storages.put(ExcessReplica.class, new ExcessReplicaDerby());
    storages.put(InvalidatedBlock.class, new InvalidatedBlockDerby());
    storages.put(Lease.class, new LeaseDerby());
    storages.put(LeasePath.class, new LeasePathDerby());
    storages.put(PendingBlockInfo.class, new PendingBlockDerby());
    INodeDerby inodeClusterj = new INodeDerby();
    storages.put(INode.class, inodeClusterj);
    storages.put(INodeDirectory.class, inodeClusterj);
    storages.put(INodeFile.class, inodeClusterj);
    storages.put(INodeDirectoryWithQuota.class, inodeClusterj);
    storages.put(INodeSymlink.class, inodeClusterj);

    return storages;
  }
}
