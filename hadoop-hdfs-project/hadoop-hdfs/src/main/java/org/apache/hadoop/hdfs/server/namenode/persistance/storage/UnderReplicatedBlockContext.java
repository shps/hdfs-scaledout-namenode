package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.Counter;
import org.apache.hadoop.hdfs.server.namenode.persistance.Finder;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionContextException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public abstract class UnderReplicatedBlockContext implements EntityContext<UnderReplicatedBlock> {

  protected Map<Long, UnderReplicatedBlock> urBlocks = new HashMap<Long, UnderReplicatedBlock>();
  protected Map<Long, UnderReplicatedBlock> modifiedurBlocks = new HashMap<Long, UnderReplicatedBlock>();
  protected Map<Long, UnderReplicatedBlock> removedurBlocks = new HashMap<Long, UnderReplicatedBlock>();
  protected boolean allUrBlocksRead = false;

  @Override
  public void clear() {
    urBlocks.clear();
    modifiedurBlocks.clear();
    removedurBlocks.clear();
    allUrBlocksRead = false;
  }

  @Override
  public void remove(UnderReplicatedBlock entity) throws TransactionContextException {

    if (!urBlocks.containsKey(entity.getBlockId())) {
      throw new TransactionContextException("Unattached under replica [blk:" + entity.getBlockId() + ", level: " + entity.getLevel() + " ] passed to be removed");
    }
    urBlocks.remove(entity.getBlockId());
    modifiedurBlocks.remove(entity.getBlockId());
    removedurBlocks.put(entity.getBlockId(), entity);
  }

  @Override
  public Collection<UnderReplicatedBlock> findList(Finder<UnderReplicatedBlock> finder, Object... params) {
    UnderReplicatedBlock.Finder urFinder = (UnderReplicatedBlock.Finder) finder;
    List<UnderReplicatedBlock> finalList = null;
    switch (urFinder) {
      case AllSortedByLevel:
        if (allUrBlocksRead) {
          finalList = new ArrayList(urBlocks.values());
        } else {
          finalList = findAllSortedByLevel();
          List<UnderReplicatedBlock> synced = syncUnderReplicatedBlockInstances(finalList);
          allUrBlocksRead = true;
          finalList = synced;
        }
        break;
      case ByLevel:
        Integer level = (Integer) params[0];
        if (allUrBlocksRead) {
          List<UnderReplicatedBlock> list = new ArrayList<UnderReplicatedBlock>();
          for (UnderReplicatedBlock block : urBlocks.values()) {
            if (block.getLevel() == level) {
              list.add(block);
            }
          }

          return list;
        }
        return syncUnderReplicatedBlockInstances(findByLevel(level));
    }

    return finalList;
  }

  @Override
  public UnderReplicatedBlock find(Finder<UnderReplicatedBlock> finder, Object... params) {
    UnderReplicatedBlock.Finder urFinder = (UnderReplicatedBlock.Finder) finder;
    UnderReplicatedBlock result = null;
    switch (urFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        if (urBlocks.containsKey(blockId)) {
          return urBlocks.get(blockId);
        }
        UnderReplicatedBlock block = findByBlockId(blockId);
        if (block != null) {
          urBlocks.put(block.getBlockId(), block);
        }
        return block;
    }

    return result;
  }

  @Override
  public int count(Counter counter, Object... params) {
    UnderReplicatedBlock.Counter urCounter = (UnderReplicatedBlock.Counter) counter;

    switch (urCounter) {
      case All:
        return findList(UnderReplicatedBlock.Finder.AllSortedByLevel).size();
      case ByLevel:
        Integer level = (Integer) params[0];
        if (allUrBlocksRead) {
          int count = 0;
          for (UnderReplicatedBlock block : urBlocks.values()) {
            if (block.getLevel() == level) {
              count++;
            }
          }

          return count;
        }
        return syncUnderReplicatedBlockInstances(findByLevel(level)).size();
      case LessThanLevel:
        level = (Integer) params[0];
        if (allUrBlocksRead) {
          int count = 0;
          for (UnderReplicatedBlock block : urBlocks.values()) {
            if (block.getLevel() < level) {
              count++;
            }
          }

          return count;
        }

        return syncUnderReplicatedBlockInstances(findAllLessThanLevel(level)).size();
    }

    return -1;
  }

  @Override
  public void update(UnderReplicatedBlock entity) throws TransactionContextException {
    if (removedurBlocks.get(entity.getBlockId()) != null) {
      throw new TransactionContextException("Removed under replica passed to be persisted");
    }

    urBlocks.put(entity.getBlockId(), entity);
    modifiedurBlocks.put(entity.getBlockId(), entity);
  }

  @Override
  public void add(UnderReplicatedBlock entity) throws TransactionContextException {
    update(entity);
  }

  protected abstract UnderReplicatedBlock findByBlockId(long blockId);

  protected abstract List<UnderReplicatedBlock> findAllSortedByLevel();

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

  protected abstract List<UnderReplicatedBlock> findByLevel(int level);

  protected abstract List<UnderReplicatedBlock> findAllLessThanLevel(int level);
}
