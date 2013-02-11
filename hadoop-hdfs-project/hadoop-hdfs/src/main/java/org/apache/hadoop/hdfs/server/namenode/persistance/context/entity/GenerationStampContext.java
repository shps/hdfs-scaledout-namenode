package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.blockmanagement.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.GenerationStampDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class GenerationStampContext extends EntityContext<GenerationStamp> {

  private GenerationStamp generationStamp = null;
  private GenerationStampDataAccess da;

  public GenerationStampContext(GenerationStampDataAccess da) {
    this.da = da;
  }

  @Override
  public void add(GenerationStamp entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clear() {
    generationStamp = null;
  }

  @Override
  public int count(CounterType<GenerationStamp> counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public GenerationStamp find(FinderType<GenerationStamp> finder, Object... params) throws PersistanceException {
    GenerationStamp.Finder gFinder = (GenerationStamp.Finder) finder;
    switch (gFinder) {
      case Counter:
        if (generationStamp == null) {
          generationStamp = new GenerationStamp(da.findCounter());
          log("find-generationstamp", CacheHitState.LOSS);
        } else {
          log("find-generationstamp", CacheHitState.HIT);
        }
        return generationStamp;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<GenerationStamp> findList(FinderType<GenerationStamp> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void prepare() throws StorageException {
    if (generationStamp != null) {
      da.prepare(generationStamp.getCounter());
    }
  }

  @Override
  public void remove(GenerationStamp entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(GenerationStamp gs) throws PersistanceException {
    this.generationStamp = gs;
    log(
        "updated-block-generationstamp",
        CacheHitState.NA,
        new String[]{"gs", Long.toString(this.generationStamp.getCounter())});
  }
}
