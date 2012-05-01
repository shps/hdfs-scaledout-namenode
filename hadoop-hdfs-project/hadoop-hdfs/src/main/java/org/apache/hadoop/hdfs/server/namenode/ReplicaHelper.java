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


/** This is a helper class for manipulating the ReplicasUnderConstruction stored in DB. 
 * Table name: ReplicaUc
 * 
 *  @author wmalik
 */
public class ReplicaHelper {

  private static final Log LOG = LogFactory.getLog(ReplicaHelper.class);
  private static final int RETRY_COUNT = 3; 


  /** insert a ReplicaUnderConstruction object in the database 
   * @param blockId
   * @param expLocation
   * @param repState
   * @throws IOException 
   */
  public static void add(long blockId, DatanodeDescriptor expLocation, ReplicaState repState) throws IOException {
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


  /** Delete all replicas of a blockId
   * @param blockId
   */
  public static void delete(long blockId) {
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
   * @return number of replicas
   */
  public static int size(long blockId) {
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
   */
  public static List<ReplicaUnderConstruction> getReplicas(long blockId) { 
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
    ReplicaUcTable rept = session.newInstance(ReplicaUcTable.class);
    rept.setBlockId(blockId);
    rept.setExpectedLocation(expLocation);
    rept.setState(repState);
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
    for(ReplicaUcTable rept : replicas)
      session.deletePersistent(rept);
  }


}