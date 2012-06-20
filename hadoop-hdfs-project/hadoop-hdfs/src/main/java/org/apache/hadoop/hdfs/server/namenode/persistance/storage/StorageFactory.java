package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.rmi.dgc.Lease;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.BlockInfoClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ExcessReplicaClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.IndexedReplicaClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.InvalidatedBlockClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.LeaseClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.LeasePathClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.PendingBlockClusterj;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.ReplicaUnderConstructionClusterj;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageFactory {

  public enum StorageType {

    Clusterj, ApacheDerby, Mysql;
  }

  public static Map<Class, Storage> getStorageMap(StorageType type) {
    Map<Class, Storage> storageMap = null;

    switch (type) {
      case Clusterj:
        storageMap = getClusterJStorage();
        break;
      case ApacheDerby:
        break;
      case Mysql:
        break;
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

    return storages;
  }
}
