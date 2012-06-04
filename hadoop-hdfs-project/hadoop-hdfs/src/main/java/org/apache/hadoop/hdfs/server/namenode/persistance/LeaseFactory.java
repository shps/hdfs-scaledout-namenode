package org.apache.hadoop.hdfs.server.namenode.persistance;

import org.apache.hadoop.hdfs.server.namenode.Lease;
import se.sics.clusterj.LeaseTable;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeaseFactory {

  public static Lease createLease(LeaseTable lTable) {
    return new Lease(lTable.getHolder(), lTable.getHolderID(), lTable.getLastUpdated());
  }
}
