package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Hooman <hooman@sics.se>
 */
public class HelperMetrics {

  private static final Log LOG = LogFactory.getLog(HelperMetrics.class);
  public static INodeMetrics inodeMetrics;
  public static BlockMetrics blockMetrics;
  public static TripleteMetrics tripleteMetrics;
  public static LeaseMetrics leaseMetrics;
  public static LeasePathMetrics leasePathMetrics;
  public static ReplicaMetrics replicaMetrics;
  public static SecretMetrics secretMetrics;
  public static BlockTotalMetrics totalMetrics;

  static {
    inodeMetrics = new INodeMetrics();
    blockMetrics = new BlockMetrics();
    tripleteMetrics = new TripleteMetrics();
    leaseMetrics = new LeaseMetrics();
    leasePathMetrics = new LeasePathMetrics();
    replicaMetrics = new ReplicaMetrics();
    secretMetrics = new SecretMetrics();
    totalMetrics = new BlockTotalMetrics();
  }

  public static void reset() {
    inodeMetrics.reset();
    blockMetrics.reset();
    tripleteMetrics.reset();
    leaseMetrics.reset();
    leasePathMetrics.reset();
    totalMetrics.reset();
    replicaMetrics.reset();
    secretMetrics.reset();
  }

  public static void printAll(String opName) {
    LOG.info(opName);
    LOG.info(inodeMetrics.toString());
    LOG.info(blockMetrics.toString());
    LOG.info(tripleteMetrics.toString());
    LOG.info(leaseMetrics.toString());
    LOG.info(leasePathMetrics.toString());
    //LOG.info(totalMetrics.toString());
    LOG.info(replicaMetrics.toString());
    //LOG.info(secretMetrics.toString());
  }
  
  public static void printAllAndReset(String opName) {
    printAll(opName);
    reset();
  }
  
}
