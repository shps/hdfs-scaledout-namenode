package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockTokenKeyDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockTokenKeyClusterj extends BlockTokenKeyDataAccess {

  @Override
  public BlockKey findByKeyType(short keyType) throws StorageException {
    Session session = connector.obtainSession();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<BlockKeyDTO> dobj = qb.createQueryDefinition(BlockKeyDTO.class);
    PredicateOperand field = dobj.get("keyType");
    Predicate predicate = field.equal(dobj.param("param1"));
    dobj.where(predicate);
    Query<BlockKeyDTO> query = session.createQuery(dobj);
    query.setParameter("param1", keyType);
    List<BlockKeyDTO> results = query.getResultList();
    if (results == null || results.isEmpty()) {
      return null;
    } else if (results.size() > 1) {
      throw new StorageException("More than 1 keys found for KeyType "
              + keyType + " - This should never happen or the world will end");
    } else {
      try {
        return createBlockKey(results.get(0));
      } catch (IOException e) {
        throw new StorageException(e);
      }
    }
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockKeyDTO {

    @PrimaryKey
    @Column(name = KEY_ID)
    int getKeyId();

    void setKeyId(int keyId);

    @Column(name = EXPIRY_DATE)
    long getExpiryDate();

    void setExpiryDate(long expiryDate);

    @Column(name = KEY_BYTES)
    byte[] getKeyBytes();

    void setKeyBytes(byte[] keyBytes);

    @Column(name = KEY_TYPE)
    short getKeyType();

    void setKeyType(short keyType);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public BlockKey findByKeyId(int keyId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      BlockKeyDTO dk = session.find(BlockKeyDTO.class, keyId);
      if (dk == null) {
        return null;
      }
      return createBlockKey(dk);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public List<BlockKey> findAll() throws StorageException {
    List<BlockKey> blockKeys = new ArrayList<BlockKey>();
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockKeyDTO> dobj = qb.createQueryDefinition(BlockKeyDTO.class);
      Query<BlockKeyDTO> query = session.createQuery(dobj);
      List<BlockKeyDTO> storedKeys = query.getResultList();
      for (BlockKeyDTO key : storedKeys) {
        blockKeys.add(createBlockKey(key));
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
    return blockKeys;
  }

  @Override
  public void prepare(Collection<BlockKey> removed, Collection<BlockKey> newed, Collection<BlockKey> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (BlockKey key : removed) {
        BlockKeyDTO kTable = session.newInstance(BlockKeyDTO.class, key.getKeyId());
        session.deletePersistent(kTable);
      }

      for (BlockKey key : newed) {
        BlockKeyDTO kTable = session.newInstance(BlockKeyDTO.class);
        createPersistable(key, kTable);
        session.savePersistent(kTable);
      }

      for (BlockKey key : modified) {
        BlockKeyDTO kTable = session.newInstance(BlockKeyDTO.class);
        createPersistable(key, kTable);
        session.savePersistent(kTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private BlockKey createBlockKey(BlockKeyDTO dk) throws IOException {
    DataInputStream dis =
            new DataInputStream(new ByteArrayInputStream(dk.getKeyBytes()));
    BlockKey bKey = new BlockKey();
    bKey.readFields(dis);

    return bKey;
  }

  private void createPersistable(BlockKey key, BlockKeyDTO kTable) throws IOException {
    kTable.setExpiryDate(key.getExpiryDate());
    kTable.setKeyId(key.getKeyId());
    kTable.setKeyType(key.getKeyType());
    DataOutputBuffer keyBytes = new DataOutputBuffer();
    key.write(keyBytes);
    kTable.setKeyBytes(keyBytes.getData());
  }

  @Override
  public void removeAll() throws StorageException {
    Session session = connector.obtainSession();
    session.deletePersistentAll(BlockKeyDTO.class);
  }
}
