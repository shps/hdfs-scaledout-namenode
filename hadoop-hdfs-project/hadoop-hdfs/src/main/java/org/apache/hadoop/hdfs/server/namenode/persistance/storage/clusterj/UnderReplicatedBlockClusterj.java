package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.UnderReplicatedBlockContext;

@PersistenceCapable(table = "under_replicated_blocks")
interface UnderReplicatedBlocksTable {

  @PrimaryKey
  @Column(name = "blockId")
  long getBlockId();

  void setBlockId(long bid);

  @Column(name = "level")
  int getLevel();

  void setLevel(int level);
}

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class UnderReplicatedBlockClusterj extends UnderReplicatedBlockContext {

  Session session = ClusterjConnector.INSTANCE.obtainSession();

  @Override
  protected UnderReplicatedBlock findByBlockId(long blockId) {
    UnderReplicatedBlocksTable urbt = session.find(UnderReplicatedBlocksTable.class, blockId);
    if (urbt == null) {
      return null;
    }
    return createUrBlock(urbt);
  }

  @Override
  public void prepare() {
    for (UnderReplicatedBlock urBlock : removedurBlocks.values()) {
      session.deletePersistent(UnderReplicatedBlocksTable.class, urBlock.getBlockId());
    }

    for (UnderReplicatedBlock urBlock : modifiedurBlocks.values()) {
      UnderReplicatedBlocksTable newInstance = session.newInstance(UnderReplicatedBlocksTable.class);
      createPersistable(urBlock, newInstance);
      session.savePersistent(newInstance);
    }
  }

  private void createPersistable(UnderReplicatedBlock block, UnderReplicatedBlocksTable persistable) {
    persistable.setBlockId(block.getBlockId());
    persistable.setLevel(block.getLevel());
  }

  private UnderReplicatedBlock createUrBlock(UnderReplicatedBlocksTable bit) {
    UnderReplicatedBlock block = new UnderReplicatedBlock(bit.getLevel(), bit.getBlockId());
    return block;
  }

  private List<UnderReplicatedBlock> createUrBlockList(List<UnderReplicatedBlocksTable> bitList) {
    List<UnderReplicatedBlock> blocks = new ArrayList<UnderReplicatedBlock>();
    for (UnderReplicatedBlocksTable bit : bitList) {
      blocks.add(createUrBlock(bit));
    }
    return blocks;
  }

  @Override
  protected List<UnderReplicatedBlock> findAllSortedByLevel() {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<UnderReplicatedBlocksTable> dobj = qb.createQueryDefinition(UnderReplicatedBlocksTable.class);
    Query<UnderReplicatedBlocksTable> query = session.createQuery(dobj);
    List<UnderReplicatedBlocksTable> urbks = query.getResultList();
    List<UnderReplicatedBlock> blocks = createUrBlockList(urbks);
    Collections.sort(blocks, UnderReplicatedBlock.Order.ByLevel);
    return blocks;
  }

  @Override
  protected List<UnderReplicatedBlock> findByLevel(int level) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<UnderReplicatedBlocksTable> dobj = qb.createQueryDefinition(UnderReplicatedBlocksTable.class);
    Predicate pred = dobj.get("level").equal(dobj.param("level"));
    dobj.where(pred);
    Query<UnderReplicatedBlocksTable> query = session.createQuery(dobj);
    query.setParameter("level", level);
    return createUrBlockList(query.getResultList());
  }

  @Override
  protected List<UnderReplicatedBlock> findAllLessThanLevel(int level) {
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<UnderReplicatedBlocksTable> dobj = qb.createQueryDefinition(UnderReplicatedBlocksTable.class);
    Predicate pred = dobj.get("level").lessThan(dobj.param("level"));
    dobj.where(pred);
    Query<UnderReplicatedBlocksTable> query = session.createQuery(dobj);
    query.setParameter("level", level);

    return createUrBlockList(query.getResultList());
  }

  @Override
  public void removeAll() throws TransactionContextException {
    removedurBlocks.clear();
    urBlocks.clear();
    modifiedurBlocks.clear();
    session.deletePersistentAll(UnderReplicatedBlocksTable.class);
  }
}
