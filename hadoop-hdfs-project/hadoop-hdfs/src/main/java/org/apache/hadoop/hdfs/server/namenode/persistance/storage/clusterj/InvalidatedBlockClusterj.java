package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.InvalidateBlockDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockClusterj extends InvalidateBlockDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface InvalidateBlocksDTO {

    @PrimaryKey
    @Column(name = STORAGE_ID)
    String getStorageId();

    void setStorageId(String storageId);

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long storageId);

    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();

    void setGenerationStamp(long generationStamp);

    @Column(name = NUM_BYTES)
    long getNumBytes();

    void setNumBytes(long numBytes);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public int countAll() throws StorageException {
    return findAllInvalidatedBlocks().size();
  }

  @Override
  public List<InvalidatedBlock> findAllInvalidatedBlocks() throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      return createList(session.createQuery(qdt).getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<InvalidatedBlock> findInvalidatedBlockByStorageId(String storageId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<InvalidateBlocksDTO> qdt = qb.createQueryDefinition(InvalidateBlocksDTO.class);
      qdt.where(qdt.get("storageId").equal(qdt.param("param")));
      Query<InvalidateBlocksDTO> query = session.createQuery(qdt);
      query.setParameter("param", storageId);
      return createList(query.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public InvalidatedBlock findInvBlockByPkey(Object[] params) throws StorageException {
    try {
      Session session = connector.obtainSession();
      InvalidateBlocksDTO invTable = session.find(InvalidateBlocksDTO.class, params);
      if (invTable == null) {
        return null;
      }
      return createReplica(invTable);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<InvalidatedBlock> removed, Collection<InvalidatedBlock> newed, Collection<InvalidatedBlock> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (InvalidatedBlock invBlock : newed) {
        InvalidateBlocksDTO newInstance = session.newInstance(InvalidateBlocksDTO.class);
        createPersistable(invBlock, newInstance);
        session.savePersistent(newInstance);
      }

      for (InvalidatedBlock invBlock : removed) {
        Object[] pk = new Object[2];
        pk[0] = invBlock.getBlockId();
        pk[1] = invBlock.getStorageId();
        session.deletePersistent(InvalidateBlocksDTO.class, pk);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private List<InvalidatedBlock> createList(List<InvalidateBlocksDTO> dtoList) {
    List<InvalidatedBlock> list = new ArrayList<InvalidatedBlock>();
    for (InvalidateBlocksDTO dto : dtoList) {
      list.add(createReplica(dto));
    }
    return list;
  }

  private InvalidatedBlock createReplica(InvalidateBlocksDTO invBlockTable) {
    return new InvalidatedBlock(invBlockTable.getStorageId(), invBlockTable.getBlockId(),
            invBlockTable.getGenerationStamp(), invBlockTable.getNumBytes());
  }

  private void createPersistable(InvalidatedBlock invBlock, InvalidateBlocksDTO newInvTable) {
    newInvTable.setBlockId(invBlock.getBlockId());
    newInvTable.setStorageId(invBlock.getStorageId());
    newInvTable.setGenerationStamp(invBlock.getGenerationStamp());
    newInvTable.setNumBytes(invBlock.getNumBytes());
  }
}
