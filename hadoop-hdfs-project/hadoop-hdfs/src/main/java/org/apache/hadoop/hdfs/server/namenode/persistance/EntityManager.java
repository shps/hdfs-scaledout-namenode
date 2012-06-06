package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.Replica;

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

  public List<Replica> findReplicasByBlockId(long id) {
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

  public InvalidatedBlock findInvalidatedBlockByPK(String storageId, long blockId) {
    try {
      return context().findInvalidatedBlockByPK(storageId, blockId);
    } catch (TransactionContextException ex) {
      Logger.getLogger(EntityManager.class.getName()).log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public Map<String, List<InvalidatedBlock>> findAllInvalidatedBlocks() {
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
}
