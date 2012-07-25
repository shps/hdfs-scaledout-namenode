package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ReplicaUnderConstruntionDataAccess;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionContext extends EntityContext<ReplicaUnderConstruction> {

  /**
   * Mappings
   */
  private Map<ReplicaUnderConstruction, ReplicaUnderConstruction> newReplicasUc = new HashMap<ReplicaUnderConstruction, ReplicaUnderConstruction>();
  private Map<ReplicaUnderConstruction, ReplicaUnderConstruction> removedReplicasUc = new HashMap<ReplicaUnderConstruction, ReplicaUnderConstruction>();
  private Map<Long, List<ReplicaUnderConstruction>> blockReplicasUc = new HashMap<Long, List<ReplicaUnderConstruction>>();
  private ReplicaUnderConstruntionDataAccess dataAccess;

  public ReplicaUnderConstructionContext(ReplicaUnderConstruntionDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(ReplicaUnderConstruction replica) throws PersistanceException {
    if (removedReplicasUc.containsKey(replica)) {
      throw new TransactionContextException("Removed  under constructionreplica passed to be persisted");
    }

    newReplicasUc.put(replica, replica);
    log("added-replicauc", CacheHitState.NA, 
            new String[]{"bid", Long.toString(replica.getBlockId()), 
              "sid", replica.getStorageId(), "state", replica.getState().name()});
  }

  @Override
  public void clear() {
    newReplicasUc.clear();
    removedReplicasUc.clear();
    blockReplicasUc.clear();
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<ReplicaUnderConstruction> findList(FinderType<ReplicaUnderConstruction> finder, Object... params) throws PersistanceException {

    ReplicaUnderConstruction.Finder rFinder = (ReplicaUnderConstruction.Finder) finder;
    List<ReplicaUnderConstruction> result = null;
    switch (rFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        if (blockReplicasUc.containsKey(blockId)) {
          log("find-replicaucs-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId)});
          result = blockReplicasUc.get(blockId);
        } else {
          log("find-replicaucs-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId)});
          result = dataAccess.findReplicaUnderConstructionByBlockId(blockId);
          blockReplicasUc.put(blockId, result);
        }
        break;
    }

    return result;
  }

  @Override
  public ReplicaUnderConstruction find(FinderType<ReplicaUnderConstruction> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedReplicasUc.values(), newReplicasUc.values(), null);
  }

  @Override
  public void remove(ReplicaUnderConstruction replica) throws PersistanceException {
    newReplicasUc.remove(replica);
    removedReplicasUc.put(replica, replica);
        log("removed-replicauc", CacheHitState.NA, 
            new String[]{"bid", Long.toString(replica.getBlockId()), 
              "sid", replica.getStorageId(), "state", replica.getState().name()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void update(ReplicaUnderConstruction replica) throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }
}
