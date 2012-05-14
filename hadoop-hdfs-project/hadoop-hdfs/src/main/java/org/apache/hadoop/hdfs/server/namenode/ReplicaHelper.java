package org.apache.hadoop.hdfs.server.namenode;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.io.DataOutputBuffer;

import se.sics.clusterj.ReplicaUcTable;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.apache.hadoop.hdfs.server.namenode.metrics.HelperMetrics;


/** This is a helper class for manipulating the ReplicasUnderConstruction stored in DB. 
 * Table name: ReplicaUc
 * 
 *  @author wmalik
 */
public class ReplicaHelper {

  private static final Log LOG = LogFactory.getLog(ReplicaHelper.class);
  private static final int RETRY_COUNT = 3; 

  public static void add(long blockId, DatanodeDescriptor expLocation, ReplicaState repState, boolean isTrans) 
      throws IOException {
    //FIXME[Hooman]: should assert the state of transaction if it's active or not.
    if(isTrans)
      add(blockId, expLocation, repState);
    else
      addWithTransaction(blockId, expLocation, repState);
  }
  
  /** insert a ReplicaUnderConstruction object in the database 
   * @param blockId
   * @param expLocation
   * @param repState
   * @throws IOException 
   */
  private static void add(long blockId, DatanodeDescriptor expLocation, ReplicaState repState) throws IOException {
    Session session = DBConnector.obtainSession();

    // Serializing the DatanodeDescriptor. Upcasting to DatanodeID 
    // because we would only need ip:port, storageID 
    DataOutputBuffer expBytes = new DataOutputBuffer();
    DatanodeID dnId = (DatanodeID) expLocation;
    dnId.write(expBytes);

    //Serializing the ReplicaState
    DataOutputBuffer repStateBuf = new DataOutputBuffer();
    repState.write(repStateBuf);

    insert(session, blockId, expBytes.getData(), repStateBuf.getData());
    session.flush();

  }

  /** insert a ReplicaUnderConstruction object in the database 
   * @param blockId
   * @param expLocation
   * @param repState
   * @throws IOException 
   */
  private static void addWithTransaction(long blockId, DatanodeDescriptor expLocation, ReplicaState repState) throws IOException {
    int tries = RETRY_COUNT;
    boolean done = false;
    Session session = DBConnector.obtainSession();
    Transaction tx = session.currentTransaction();
    
    // Serializing the DatanodeDescriptor. Upcasting to DatanodeID 
    // because we would only need ip:port, storageID 
    DataOutputBuffer expBytes = new DataOutputBuffer();
    DatanodeID dnId = (DatanodeID) expLocation;
    dnId.write(expBytes);

    //Serializing the ReplicaState
    DataOutputBuffer repStateBuf = new DataOutputBuffer();
    repState.write(repStateBuf);
    
    while(done == false && tries > 0) {
      try {   
        tx.begin();
        insert(session, blockId, expBytes.getData(), repStateBuf.getData());
        tx.commit();
        session.flush();
        done = true;
      }
      catch(ClusterJException e) {
        if(tx.isActive())
          tx.rollback();
        e.printStackTrace();
        tries--;
      }
    }
  }


  public static void delete(long blockId, boolean isTransactional) {
    if(isTransactional)
      delete(blockId);
    else
      deleteWithTransaction(blockId);
  }
  
  
  /** Delete all replicas of a blockId
   * @param blockId
   */
  private static void delete(long blockId) {
    Session session = DBConnector.obtainSession();
    delete(session, blockId);
    session.flush();
  }
  
  /** Delete all replicas of a blockId
   * @param blockId
   */
  private static void deleteWithTransaction(long blockId) {
    int tries = RETRY_COUNT;
    boolean done = false;
    Session session = DBConnector.obtainSession();
    Transaction tx = session.currentTransaction();

    while(done == false && tries > 0) {
      try {   
        tx.begin();
        delete(session, blockId);
        tx.commit();
        session.flush();
        done = true;
      }
      catch(ClusterJException e) {
        if(tx.isActive())
          tx.rollback();
        e.printStackTrace();
        tries--;
      } 
    }
  }
  
  
  
  /** The number of replicas for a given blockId
   * @param blockId
   * @param isTransactional
   * @return
   */
  public static int size(long blockId, boolean isTransactional) {
    if(isTransactional)
      return size(blockId);
    else
      return sizeWithTransaction(blockId);
  }

  
  
  /** The number of replicas for a given blockId
   * @param blockId
   * @return number of replicas
   */
  private static int size(long blockId) {
    Session session = DBConnector.obtainSession();
    return select(session, blockId).size();
  }
  

  /** The number of replicas for a given blockId
   * @param blockId
   * @return number of replicas
   */
  private static int sizeWithTransaction(long blockId) {
    int tries = RETRY_COUNT;
    boolean done = false;
    Session session = DBConnector.obtainSession();
    while(done == false && tries > 0) {
      try {   
        return select(session, blockId).size();
      }
      catch(ClusterJException e) {
        e.printStackTrace();
        tries--;
      } 
    }
    return -1;
  }


  /** Fetch all replicas of a blockId from DB. 
   * This method will never return null
   * @param blockId
   * @return a list of ReplicaUnderConstruction objects
   * @throws IOException 
   */
  public static List<ReplicaUnderConstruction> getReplicas(long blockId, boolean isTransactional) throws IOException { 
    if(isTransactional) //FIXME[Hooman]: Why is this using isTransactional? There is no need for a read operation.
      return getReplicas(blockId);
    else
      return getReplicasWithTransaction(blockId);
  }
  
  /** Fetch all replicas of a blockId from DB. 
   * This method will never return null
   * @param blockId
   * @return a list of ReplicaUnderConstruction objects
   * @throws IOException 
   */
  private static List<ReplicaUnderConstruction> getReplicas(long blockId) throws IOException { 
    Session session = DBConnector.obtainSession();
    List<ReplicaUcTable> repList = select(session, blockId);
    return convert(repList);
  }
  
  /** Fetch all replicas of a blockId from DB. 
   * This method will never return null
   * @param blockId
   * @return a list of ReplicaUnderConstruction objects
   */
  private static List<ReplicaUnderConstruction> getReplicasWithTransaction(long blockId) { 
    int tries = RETRY_COUNT;
    boolean done = false;
    Session session = DBConnector.obtainSession();
    while(done == false && tries > 0) {
      try {   
        List<ReplicaUcTable> repList = select(session, blockId);
        return convert(repList);
      }
      catch(ClusterJException e) {
        e.printStackTrace();
        tries--;
      } catch (IOException e) {
        e.printStackTrace();
      } 
    }
    return new ArrayList<ReplicaUnderConstruction>();
  }


  /** Converts a list of ClusterJ objects to a list of HDFS objects.
   * The returned list is also sorted with respect to timestamp
   * @param reptList
   * @return a sorted list of ReplicaUnderConstruction
   * @throws IOException
   */
  private static List<ReplicaUnderConstruction> convert(List<ReplicaUcTable> reptList) throws IOException {
    if(reptList == null || reptList.size() == 0)
      return new ArrayList<ReplicaUnderConstruction>();

    sortReplicas(reptList);
    List<ReplicaUnderConstruction> toReturn = new ArrayList<ReplicaUnderConstruction>(reptList.size());

    for (ReplicaUcTable rept : reptList) {
      // fetching a blockinfo from database and upcasting to Block
      Block block = (Block)BlocksHelper.getBlockInfoSingle(rept.getBlockId());

      //Deserializing the datanode descriptor
      DataInputStream dis = new DataInputStream(
          new ByteArrayInputStream(rept.getExpectedLocation()));
      DatanodeID target = new DatanodeID();
      target.readFields(dis);

      //Deserializing the ReplicaState
      DataInputStream stateStream = new DataInputStream(new ByteArrayInputStream(rept.getState()));

      toReturn.add(
          new ReplicaUnderConstruction(
              block,
              new DatanodeDescriptor(target),
              ReplicaState.read(stateStream)));
    }

    return toReturn;

  }

  /** Sort the BlockUcTable objects according to timestamp (ascending)
   * @param reptList
   */
  private static void sortReplicas(List<ReplicaUcTable> reptList) {
    Collections.sort(reptList,
        new Comparator<ReplicaUcTable>() {
      @Override
      public int compare(ReplicaUcTable o1, ReplicaUcTable o2) {
        return o1.getTimestamp() < o2.getTimestamp() ? -1 : 1;
      }
    }
        );
  }


  ////////////////////////////////////////////////////////////////////////
  // 				Internal Methods
  ////////////////////////////////////////////////////////////////////////


  
  /** Inserts a row in the BlockUc table
   * @param session
   * @param blockId
   * @param expLocation
   * @param repState
   */
  private static void insert(Session session, long blockId, byte[] expLocation, byte[] repState) {
    HelperMetrics.replicaMetrics.incrInsert();
    
    ReplicaUcTable rept = session.newInstance(ReplicaUcTable.class);
    rept.setBlockId(blockId);
    rept.setExpectedLocation(expLocation);
    rept.setState(repState);
    rept.setId(DFSUtil.getRandom().nextInt());
    //setting the timestamp for ordering
    rept.setTimestamp(System.currentTimeMillis());
    session.makePersistent(rept);
  }


  /** Fetch all rows for a blockId
   * @param session
   * @param blockId
   * @return a list of ReplicaUcTable objects
   */
  private static List<ReplicaUcTable> select(Session session, long blockId) {
    HelperMetrics.replicaMetrics.incrSelectUsingIndex();
    
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<ReplicaUcTable> dobj = qb.createQueryDefinition(ReplicaUcTable.class);
    dobj.where(dobj.get("blockId").equal(dobj.param("param")));
    Query<ReplicaUcTable> query = session.createQuery(dobj);
    query.setParameter("param", blockId);
    return  query.getResultList();
  }


  /** Delete all rows with the given blockId
   * @param session
   * @param blockId
   */
  private static void delete(Session session, long blockId) {		
    List<ReplicaUcTable> replicas = select(session, blockId);
    
    HelperMetrics.replicaMetrics.incrDelete(); //[Hooman]: it does not flush, considered as one roundtrip.
    
    for(ReplicaUcTable rept : replicas)
      session.deletePersistent(rept);
  }


}