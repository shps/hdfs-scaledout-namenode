package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.UnderReplicatedBlockDataAccess;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class UnderReplicatedBlockContext extends EntityContext<UnderReplicatedBlock> {

  private Map<Long, UnderReplicatedBlock> urBlocks = new HashMap<Long, UnderReplicatedBlock>();
  private Map<Long, UnderReplicatedBlock> newurBlocks = new HashMap<Long, UnderReplicatedBlock>();
  private Map<Long, UnderReplicatedBlock> modifiedurBlocks = new HashMap<Long, UnderReplicatedBlock>();
  private Map<Long, UnderReplicatedBlock> removedurBlocks = new HashMap<Long, UnderReplicatedBlock>();
  private boolean allUrBlocksRead = false;
  private UnderReplicatedBlockDataAccess dataAccess;

  public UnderReplicatedBlockContext(UnderReplicatedBlockDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(UnderReplicatedBlock entity) throws PersistanceException {
    if (removedurBlocks.get(entity.getBlockId()) != null) {
      throw new TransactionContextException("Removed under replica passed to be persisted");
    }

    urBlocks.put(entity.getBlockId(), entity);
    newurBlocks.put(entity.getBlockId(), entity);
    log("added-urblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()),
              "level", Integer.toString(entity.getLevel())});
  }

  @Override
  public void clear() {
    urBlocks.clear();
    newurBlocks.clear();
    modifiedurBlocks.clear();
    removedurBlocks.clear();
    allUrBlocksRead = false;
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    UnderReplicatedBlock.Counter urCounter = (UnderReplicatedBlock.Counter) counter;

    switch (urCounter) {
      case All:
        log("count-all-urblocks");
        return findList(UnderReplicatedBlock.Finder.AllSortedByLevel).size();
      case ByLevel:
        Integer level = (Integer) params[0];
        if (allUrBlocksRead) {
          log("count-urblocks-by-level", CacheHitState.HIT, new String[]{Integer.toString(level)});
          int count = 0;
          for (UnderReplicatedBlock block : urBlocks.values()) {
            if (block.getLevel() == level) {
              count++;
            }
          }

          return count;
        }
        log("count-urblocks-by-level", CacheHitState.LOSS, new String[]{Integer.toString(level)});
        return syncUnderReplicatedBlockInstances(dataAccess.findByLevel(level)).size();
      case LessThanLevel:
        level = (Integer) params[0];
        if (allUrBlocksRead) {
          log("count-urblocks-less-than-level", CacheHitState.HIT, new String[]{Integer.toString(level)});
          int count = 0;
          for (UnderReplicatedBlock block : urBlocks.values()) {
            if (block.getLevel() < level) {
              count++;
            }
          }

          return count;
        }
        log("count-urblocks-less-than-level", CacheHitState.LOSS, new String[]{Integer.toString(level)});
        return syncUnderReplicatedBlockInstances(dataAccess.findAllLessThanLevel(level)).size();
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public Collection<UnderReplicatedBlock> findList(FinderType<UnderReplicatedBlock> finder, Object... params) throws PersistanceException {
    UnderReplicatedBlock.Finder urFinder = (UnderReplicatedBlock.Finder) finder;
    List<UnderReplicatedBlock> finalList = null;
    switch (urFinder) {
      case AllSortedByLevel:
        if (allUrBlocksRead) {
          log("find-all-urblocks", CacheHitState.HIT);
          finalList = new ArrayList(urBlocks.values());
        } else {
          log("find-all-urblocks", CacheHitState.LOSS);
          finalList = dataAccess.findAllSortedByLevel();
          List<UnderReplicatedBlock> synced = syncUnderReplicatedBlockInstances(finalList);
          allUrBlocksRead = true;
          finalList = synced;
        }
        return finalList;
      case ByLevel:
        Integer level = (Integer) params[0];
        if (allUrBlocksRead) {
          log("find-urblocks-by-level", CacheHitState.HIT, new String[]{"level", Integer.toString(level)});
          List<UnderReplicatedBlock> list = new ArrayList<UnderReplicatedBlock>();
          for (UnderReplicatedBlock block : urBlocks.values()) {
            if (block.getLevel() == level) {
              list.add(block);
            }
          }

          return list;
        }
        log("find-urblocks-by-level", CacheHitState.LOSS, new String[]{"level", Integer.toString(level)});
        return syncUnderReplicatedBlockInstances(dataAccess.findByLevel(level));
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public UnderReplicatedBlock find(FinderType<UnderReplicatedBlock> finder, Object... params) throws PersistanceException {
    UnderReplicatedBlock.Finder urFinder = (UnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        if (urBlocks.containsKey(blockId)) {
          log("find-urblock-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId)});
          return urBlocks.get(blockId);
        }
        log("find-urblock-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId)});
        UnderReplicatedBlock block = dataAccess.findByBlockId(blockId);
        if (block != null) {
          urBlocks.put(block.getBlockId(), block);
        }
        return block;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedurBlocks.values(), newurBlocks.values(), modifiedurBlocks.values());
  }

  @Override
  public void remove(UnderReplicatedBlock entity) throws PersistanceException {

    if (!urBlocks.containsKey(entity.getBlockId())) {
      throw new TransactionContextException("Unattached under replica [blk:" + entity.getBlockId() + ", level: " + entity.getLevel() + " ] passed to be removed");
    }
    urBlocks.remove(entity.getBlockId());
    newurBlocks.remove(entity.getBlockId());
    modifiedurBlocks.remove(entity.getBlockId());
    removedurBlocks.put(entity.getBlockId(), entity);
    log("removed-urblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()),
              "level", Integer.toString(entity.getLevel())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    clear();
    dataAccess.removeAll();
    log("removed-all-urblocks");
  }

  @Override
  public void update(UnderReplicatedBlock entity) throws PersistanceException {
    if (removedurBlocks.get(entity.getBlockId()) != null) {
      throw new TransactionContextException("Removed under replica passed to be persisted");
    }

    urBlocks.put(entity.getBlockId(), entity);
    modifiedurBlocks.put(entity.getBlockId(), entity);
    log("updated-urblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()),
              "level", Integer.toString(entity.getLevel())});
  }

  private List<UnderReplicatedBlock> syncUnderReplicatedBlockInstances(List<UnderReplicatedBlock> blocks) {
    ArrayList<UnderReplicatedBlock> finalList = new ArrayList<UnderReplicatedBlock>();

    for (UnderReplicatedBlock block : blocks) {
      if (removedurBlocks.containsKey(block.getBlockId())) {
        continue;
      }
      if (urBlocks.containsKey(block.getBlockId())) {
        finalList.add(urBlocks.get(block.getBlockId()));
      } else {
        urBlocks.put(block.getBlockId(), block);
        finalList.add(block);
      }
    }

    return finalList;
  }
}
