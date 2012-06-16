package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.LeasePath;
import se.sics.clusterj.LeasePathsTable;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathFactory {

  public static LeasePath[] createLeasePaths(List<LeasePathsTable> leasePathTables) {
    LeasePath[] lPaths = new LeasePath[leasePathTables.size()];
    if (leasePathTables != null) {
      for (int i = 0; i < lPaths.length; i++) {
        LeasePathsTable lpt = leasePathTables.get(i);
        lPaths[i] = (new LeasePath(lpt.getPath(), lpt.getHolderID()));
      }
    }

    return lPaths;
  }

  public static LeasePath createLeasePath(LeasePathsTable leasePathTable) {
    return new LeasePath(leasePathTable.getPath(), leasePathTable.getHolderID());
  }

  public static void createPersistableLeasePathInstance(LeasePath lp, LeasePathsTable lTable) {
    lTable.setHolderID(lp.getHolderId());
    lTable.setPath(lp.getPath());
  }
}
