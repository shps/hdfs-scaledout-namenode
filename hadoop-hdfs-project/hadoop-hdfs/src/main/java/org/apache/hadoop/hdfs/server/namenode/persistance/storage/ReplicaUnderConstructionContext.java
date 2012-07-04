package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.persistance.Counter;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class ReplicaUnderConstructionContext implements EntityContext<ReplicaUnderConstruction> {

  public static final String TABLE_NAME = "replica_under_constructions";
  public static final String BLOCK_ID = "block_id";
  public static final String STORAGE_ID = "storage_id";
  public static final String STATE = "state";
  public static final String REPLICA_INDEX = "replica_index";
  /**
   * Mappings
   */
  protected Map<String, ReplicaUnderConstruction> newReplicasUc = new HashMap<String, ReplicaUnderConstruction>();
  protected Map<String, ReplicaUnderConstruction> removedReplicasUc = new HashMap<String, ReplicaUnderConstruction>();
  protected Map<Long, List<ReplicaUnderConstruction>> blockReplicasUc = new HashMap<Long, List<ReplicaUnderConstruction>>();

  @Override
  public void clear() {
    newReplicasUc.clear();
    removedReplicasUc.clear();
    blockReplicasUc.clear();
  }

  @Override
  public void remove(ReplicaUnderConstruction replica) throws TransactionContextException {
    newReplicasUc.remove(replica.cacheKey());
    removedReplicasUc.put(replica.cacheKey(), replica);
  }

  @Override
  public List<ReplicaUnderConstruction> findList(Finder<ReplicaUnderConstruction> finder, Object... params) {

    ReplicaUnderConstruction.Finder rFinder = (ReplicaUnderConstruction.Finder) finder;
    List<ReplicaUnderConstruction> result = null;
    switch (rFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        if (blockReplicasUc.containsKey(blockId)) {
          result = blockReplicasUc.get(blockId);
        } else {
          result = findReplicaUnderConstructionByBlockId(blockId);
          blockReplicasUc.put(blockId, result);
        }
        break;
    }

    return result;
  }

  @Override
  public ReplicaUnderConstruction find(Finder<ReplicaUnderConstruction> finder, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int count(Counter counter, Object... params) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(ReplicaUnderConstruction replica) throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void add(ReplicaUnderConstruction replica) throws TransactionContextException {
    if (removedReplicasUc.containsKey(replica.cacheKey())) {
      throw new TransactionContextException("Removed  under constructionreplica passed to be persisted");
    }

    newReplicasUc.put(replica.cacheKey(), replica);
  }

  protected abstract List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId);
}
