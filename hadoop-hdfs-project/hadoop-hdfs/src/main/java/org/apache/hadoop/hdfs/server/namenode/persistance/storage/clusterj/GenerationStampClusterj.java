package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.GenerationStampDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class GenerationStampClusterj extends GenerationStampDataAccess {

  // Generation stamp should have only a single row which stores the generation stamp number (counter),
  public static final int COUNTER_ID = 0;

  @PersistenceCapable(table = TABLE_NAME)
  public interface GenerationStampDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = COUNTER)
    long getCounter();

    void setCounter(long counter);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public Long findCounter() throws StorageException {
    try {
      Session session = connector.obtainSession();
      GenerationStampDTO gs = session.find(GenerationStampDTO.class, COUNTER_ID);
      if (gs == null) {
        throw new StorageException("There is no generation stamp entry with id " + COUNTER_ID);
      }
      return gs.getCounter();
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(long counter) throws StorageException {
    Session session = connector.obtainSession();
    GenerationStampDTO gs = session.newInstance(GenerationStampDTO.class);
    gs.setCounter(counter);
    gs.setId(COUNTER_ID);
    session.savePersistent(gs);
  }
}
