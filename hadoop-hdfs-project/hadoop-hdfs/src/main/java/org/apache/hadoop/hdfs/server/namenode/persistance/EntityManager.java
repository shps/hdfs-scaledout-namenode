package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.Replica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class EntityManager {

  private static EntityManager instance;

  private EntityManager() {
  }

  public synchronized static EntityManager getInstance() {
    if (instance == null) {
      instance = new EntityManager();
    }

    return instance;
  }
  ThreadLocal<TransactionContext> contexts = new ThreadLocal<TransactionContext>();

  private TransactionContext context() {
    TransactionContext context = contexts.get();

    if (context == null) {
      context = new TransactionContext();
      contexts.set(context);
    }
    return context;
  }

  public void persist(Object o) {
    try {
      context().persist(o);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void begin() {
    context().begin();
  }

  public void commit() {
    try {
      context().commit();
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void rollback() {
    context().rollback();
  }

  public void remove(Object obj) {
    try {
      context().remove(obj);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void removeAll(Class type) {
    try {
      context().removeAll(type);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void update(Object newValue) {
    try {
      context().update(newValue);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public List<IndexedReplica> findReplicasByBlockId(long id) {
    try {
      return context().findReplicasByBlockId(id);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  public List<BlockInfo> findBlocksByInodeId(long id) {
    try {
      try {
        return context().findBlocksByInodeId(id);
      } catch (IOException ex) {
        Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
      }
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  public BlockInfo findBlockById(long blockId) throws IOException {
    try {
      return context().findBlockById(blockId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  public List<BlockInfo> findAllBlocks() throws IOException {
    try {
      return context().findAllBlocks();
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public List<BlockInfo> findBlocksByStorageId(String name) throws IOException {
    try {
      return context().findBlocksByStorageId(name);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public List<InvalidatedBlock> findInvalidatedBlocksByStorageId(String storageId) {
    try {
      return context().findInvalidatedBlocksByStorageId(storageId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public Replica findInvalidatedBlockByPK(String storageId, long blockId) {
    try {
      return context().findInvalidatedBlockByPK(storageId, blockId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public Map<String, HashSet<InvalidatedBlock>> findAllInvalidatedBlocks() {
    try {
      return context().findAllInvalidatedBlocks();
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public long countAllInvalidatedBlocks() {
    try {
      return context().countAllInvalidatedBlocks();
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return 0;
  }

  public TreeSet<Long> findExcessReplicaByStorageId(String storageId) {
    try {
      return context().findExcessReplicaByStorageId(storageId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  /**
   * This method is only used for metrics.
   * @return
   * @throws TransactionContextException 
   */
  public long countAllExcessReplicas() {
    try {
      return context().countAllExcessReplicas();
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return 0;
  }

  public ExcessReplica findExcessReplicaByPK(String storageId, long blockId) {
    try {
      return context().findExcessReplicaByPK(storageId, blockId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }
  
  public Collection<DatanodeDescriptor> findCorruptReplica(long blockId) {
    try {
      return context().findCorruptReplica(blockId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }
  public DatanodeDescriptor findCorruptReplica(long blockId, String storageId) {
    try {
      return context().findCorruptReplica(blockId, storageId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public Collection<CorruptReplica> findAllCorruptBlocks() {
    try {
      return context().findAllCorruptBlocks();
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }
  
  public boolean containsUnderReplicatedBlock(long blockId) {
    try {
      return context().containsUnderReplicatedBlock(blockId);
    }
    catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
    return false;
  }

  public List<UnderReplicatedBlock> findAllUnderReplicatedBlocks() {
    try {
      return context().findAllUnderReplicatedBlocks();
    }
    catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
    return null;
  }

  public int countNonCorruptedUnderReplicatedBlocks(int level) {
    try {
      return context().countNonCorruptedUnderReplicatedBlocks(level);
    }
    catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
    return 0;
  }

  public int countCorruptedUnderReplicatedBlocks(int level) {
    try {
      return context().countCorruptedUnderReplicatedBlocks(level);
    }
    catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }
    return 0;
  }
        
}
