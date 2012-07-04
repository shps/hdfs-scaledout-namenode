package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Comparator;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

/**
 * ReplicaUnderConstruction contains information about replicas while they are
 * under construction. The GS, the length and the state of the replica is as
 * reported by the data-node. It is not guaranteed, but expected, that
 * data-nodes actually have corresponding replicas.
 *
 * @author Kamal Hakimzadeh <kamal@sics.se>
 */
public class ReplicaUnderConstruction extends IndexedReplica {

  public static enum Finder implements org.apache.hadoop.hdfs.server.namenode.FinderType<ReplicaUnderConstruction> {

    ByBlockId;

    @Override
    public Class getType() {
      return ReplicaUnderConstruction.class;
    }
  }

  public static enum Order implements Comparator<ReplicaUnderConstruction> {

    ByIndex() {

      @Override
      public int compare(ReplicaUnderConstruction o1, ReplicaUnderConstruction o2) {
        if (o1.getIndex() < o2.getIndex()) {
          return -1;
        } else {
          return 1;
        }
      }
    };
  }
  HdfsServerConstants.ReplicaState state;

  public ReplicaUnderConstruction(ReplicaState state, String storageId, long blockId, int index) {
    super(blockId, storageId, index);
    this.state = state;
  }

  public ReplicaState getState() {
    return state;
  }

  public void setState(ReplicaState state) {
    this.state = state;
  }
}
