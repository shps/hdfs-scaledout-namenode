package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public abstract class ExcessReplicaStorage implements Storage<ExcessReplica> {

  protected Map<ExcessReplica, ExcessReplica> exReplicas = new HashMap<ExcessReplica, ExcessReplica>();
  protected Map<String, TreeSet<ExcessReplica>> storageIdToExReplica = new HashMap<String, TreeSet<ExcessReplica>>();
  protected Map<ExcessReplica, ExcessReplica> modifiedExReplica = new HashMap<ExcessReplica, ExcessReplica>();
  protected Map<ExcessReplica, ExcessReplica> removedExReplica = new HashMap<ExcessReplica, ExcessReplica>();

  @Override
  public void clear() {
    exReplicas.clear();
    storageIdToExReplica.clear();
    modifiedExReplica.clear();
    removedExReplica.clear();
  }
}
