package org.apache.hadoop.hdfs.server.namenode.metrics;

public class HelperMetrics {

  public static INodeMetrics inodeMetrics;
  public static BlockMetrics blockMetrics;
  public static TripleteMetrics tripleteMetrics;
  public static LeaseMetrics leaseMetrics;
  public static LeasePathMetrics leasePathMetrics;
  public static ReplicaMetrics replicaMetrics;
  public static SecretMetrics secretMetrics;

  static {
    inodeMetrics = new INodeMetrics();
    blockMetrics = new BlockMetrics();
    tripleteMetrics = new TripleteMetrics();
    leaseMetrics = new LeaseMetrics();
    leasePathMetrics = new LeasePathMetrics();
    replicaMetrics = new ReplicaMetrics();
    secretMetrics = new SecretMetrics();
  }

  public static void reset() {
    inodeMetrics.reset();
    blockMetrics.reset();
    tripleteMetrics.reset();
    leaseMetrics.reset();
    leasePathMetrics.reset();
    replicaMetrics.reset();
    secretMetrics.reset();
  }
}
