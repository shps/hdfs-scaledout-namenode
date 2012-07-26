package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.UnderReplicatedBlockDataAccess;
import java.util.*;
import java.util.Map.Entry;
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
  private Map<Integer, HashSet<UnderReplicatedBlock>> levelToReplicas = new HashMap<Integer, HashSet<UnderReplicatedBlock>>();
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

    addNewReplica(entity);
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
    levelToReplicas.clear();
    allUrBlocksRead = false;
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    UnderReplicatedBlock.Counter urCounter = (UnderReplicatedBlock.Counter) counter;

    switch (urCounter) {
      case All:
        log("count-all-urblocks");
        return findList(UnderReplicatedBlock.Finder.All).size();
      case ByLevel:
        Integer level = (Integer) params[0];
        if (allUrBlocksRead) {
          log("count-urblocks-by-level", CacheHitState.HIT, new String[]{Integer.toString(level)});
        } else {
          log("count-urblocks-by-level", CacheHitState.LOSS, new String[]{Integer.toString(level)});
          syncUnderReplicatedBlockInstances(dataAccess.findByLevel(level)).size();
        }
        if (levelToReplicas.containsKey(level)) {
          return levelToReplicas.get(level).size();
        } else {
          return 0;
        }
      case LessThanLevel:
        level = (Integer) params[0];
        if (allUrBlocksRead) {
          log("count-urblocks-less-than-level", CacheHitState.HIT, new String[]{Integer.toString(level)});
        } else {
          log("count-urblocks-less-than-level", CacheHitState.LOSS, new String[]{Integer.toString(level)});
          syncUnderReplicatedBlockInstances(dataAccess.findAllLessThanLevel(level)).size();
        }
        int count = 0;
        Iterator<Entry<Integer, HashSet<UnderReplicatedBlock>>> iterator = levelToReplicas.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<Integer, HashSet<UnderReplicatedBlock>> next = iterator.next();
          if (next.getKey() < level) {
            count += next.getValue().size();
          }
        }
        return count;
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public Collection<UnderReplicatedBlock> findList(FinderType<UnderReplicatedBlock> finder, Object... params) throws PersistanceException {
    UnderReplicatedBlock.Finder urFinder = (UnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case All:
        if (allUrBlocksRead) {
          log("find-all-urblocks", CacheHitState.HIT);
        } else {
          log("find-all-urblocks", CacheHitState.LOSS);
          syncUnderReplicatedBlockInstances(dataAccess.findAll());
          allUrBlocksRead = true;
        }
        List<UnderReplicatedBlock> result = new ArrayList(urBlocks.values());
        Collections.sort(result, UnderReplicatedBlock.Order.ByLevel);
        return result;
      case ByLevel:
        Integer level = (Integer) params[0];
        if (allUrBlocksRead) {
          log("find-urblocks-by-level", CacheHitState.HIT, new String[]{"level", Integer.toString(level)});
        } else {
          log("find-urblocks-by-level", CacheHitState.LOSS, new String[]{"level", Integer.toString(level)});
          syncUnderReplicatedBlockInstances(dataAccess.findByLevel(level));
        }
        if (levelToReplicas.containsKey(level)) {
          return new ArrayList(levelToReplicas.get(level));
        } else {
          return new ArrayList<UnderReplicatedBlock>();
        }
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
    if (levelToReplicas.containsKey(entity.getLevel())) {
      levelToReplicas.get(entity.getLevel()).remove(entity);
    }
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
        addNewReplica(block);
        finalList.add(block);
      }
    }

    return finalList;
  }

  private void addNewReplica(UnderReplicatedBlock block) {
    urBlocks.put(block.getBlockId(), block);
    if (!levelToReplicas.containsKey(block.getLevel())) {
      levelToReplicas.put(block.getLevel(), new HashSet<UnderReplicatedBlock>());
    }
    levelToReplicas.get(block.getLevel()).add(block);
  }
}
