package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import org.apache.hadoop.hdfs.server.namenode.persistance.context.entity.EntityContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj.*;
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

  private static StorageConnector defaultStorage;
//  private static final StorageConnector defaultStorage = ClusterjConnector.INSTANCE;

  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    String storageType = conf.get(DFSConfigKeys.DFS_STORAGE_TYPE_KEY);
    if (storageType.equals("derby")) {
      defaultStorage = DerbyConnector.INSTANCE;
    } else if (storageType.equals("clusterj")) {
      defaultStorage = ClusterjConnector.INSTANCE;
    }
  }

  public static StorageConnector getConnector() {
    return defaultStorage;
  }

  public enum StorageType {
    Clusterj, ApacheDerby, Mysql;
  }

  public static Map<Class, EntityContext> getEntityContexts() {
    if (defaultStorage instanceof DerbyConnector) {
      return getEntityContexts(StorageType.ApacheDerby);
    } else if (defaultStorage instanceof ClusterjConnector) {
      return getEntityContexts(StorageType.Clusterj);
    } else {
      return null;
    }

  }

  private static Map<Class, EntityContext> getEntityContexts(StorageType type) {
    Map<Class, EntityContext> entityContexts = null;

    switch (type) {
      case Clusterj:
        entityContexts = getClusterJEntityContexts();
        break;
      case ApacheDerby:
        entityContexts = getDerbyEntityContexts();
        break;
      case Mysql:
        throw new UnsupportedOperationException("Mysql is not supported yet.");
    }

    return entityContexts;
  }

  private static Map<Class, EntityContext> getClusterJEntityContexts() {
    Map<Class, EntityContext> entityContexts = new HashMap<Class, EntityContext>();
    BlockInfoClusterj bicj = new BlockInfoClusterj();
    entityContexts.put(BlockInfo.class, bicj);
    entityContexts.put(BlockInfoUnderConstruction.class, bicj);
    entityContexts.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionClusterj());
    entityContexts.put(IndexedReplica.class, new IndexedReplicaClusterj());
    entityContexts.put(ExcessReplica.class, new ExcessReplicaClusterj());
    entityContexts.put(InvalidatedBlock.class, new InvalidatedBlockClusterj());
    entityContexts.put(Lease.class, new LeaseClusterj());
    entityContexts.put(LeasePath.class, new LeasePathClusterj());
    entityContexts.put(PendingBlockInfo.class, new PendingBlockClusterj());
    INodeClusterj inodeClusterj = new INodeClusterj();
    entityContexts.put(INode.class, inodeClusterj);
    entityContexts.put(INodeDirectory.class, inodeClusterj);
    entityContexts.put(INodeFile.class, inodeClusterj);
    entityContexts.put(INodeDirectoryWithQuota.class, inodeClusterj);
    entityContexts.put(INodeSymlink.class, inodeClusterj);
    entityContexts.put(CorruptReplica.class, new CorruptReplicaClusterj());
    entityContexts.put(UnderReplicatedBlock.class, new UnderReplicatedBlockClusterj());
    return entityContexts;
  }

  private static Map<Class, EntityContext> getDerbyEntityContexts() {
    Map<Class, EntityContext> entityContext = new HashMap<Class, EntityContext>();
    BlockInfoDerby bidrby = new BlockInfoDerby();
    entityContext.put(BlockInfo.class, bidrby);
    entityContext.put(BlockInfoUnderConstruction.class, bidrby);
    entityContext.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionDerby());
    entityContext.put(IndexedReplica.class, new IndexedReplicaDerby());
    entityContext.put(ExcessReplica.class, new ExcessReplicaDerby());
    entityContext.put(InvalidatedBlock.class, new InvalidatedBlockDerby());
    entityContext.put(Lease.class, new LeaseDerby());
    entityContext.put(LeasePath.class, new LeasePathDerby());
    entityContext.put(PendingBlockInfo.class, new PendingBlockDerby());
    INodeDerby inodeClusterj = new INodeDerby();
    entityContext.put(INode.class, inodeClusterj);
    entityContext.put(INodeDirectory.class, inodeClusterj);
    entityContext.put(INodeFile.class, inodeClusterj);
    entityContext.put(INodeDirectoryWithQuota.class, inodeClusterj);
    entityContext.put(INodeSymlink.class, inodeClusterj);

    return entityContext;
  }
}
