package org.apache.hadoop.hdfs.server.namenode;

import java.util.Collection;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;

/************************************************************
 * A Lease governs all the locks held by a single client.
 * For each client there's a corresponding lease, whose
 * timestamp is updated when the client periodically
 * checks in.  If the client dies and allows its lease to
 * expire, all the corresponding locks can be released.
 *************************************************************/
public class Lease implements Comparable<Lease> {

  private final String holder;
  private long lastUpdate;
  private Collection<LeasePath> paths = new TreeSet<LeasePath>();
  private int holderID; // KTHFS
  private EntityManager em = EntityManager.getInstance();

  public Lease(String holder, int holderID, long lastUpd) {
    this.holder = holder;
    this.holderID = holderID;
    this.lastUpdate = lastUpd;
  }

  /** W: Added for ThesisFS */
  public void setLastUpdate(long lastUpd) {
    this.lastUpdate = lastUpd;
  }

  public void setPaths(TreeSet<LeasePath> paths) {
    this.paths = paths;
  }

  public long getLastUpdated() {
    return this.lastUpdate;
  }

  public void setHolderID(int holderID) {
    this.holderID = holderID;
  }

  public int getHolderID() {
    return this.holderID;
  }

  public boolean removePath(LeasePath lPath) {
    return getPaths().remove(lPath);
  }

  public void addPath(LeasePath lPath) {
    getPaths().add(lPath);
  }

  /** Does this lease contain any path? */
  boolean hasPath() {
    return !this.getPaths().isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "[Lease.  Holder: " + holder
            + ", pendingcreates: " + paths.size() + "]";
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(Lease o) {
    Lease l1 = this;
    Lease l2 = o;
    long lu1 = l1.lastUpdate;
    long lu2 = l2.lastUpdate;
    if (lu1 < lu2) {
      return -1;
    } else if (lu1 > lu2) {
      return 1;
    } else {
      return l1.holder.compareTo(l2.holder);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Lease)) {
      return false;
    }
    Lease obj = (Lease) o;
    if (lastUpdate == obj.lastUpdate
            && holder.equals(obj.holder)) {
      return true;
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return holder.hashCode();
  }

  public Collection<LeasePath> getPaths() {
    if (paths == null) {
      paths = em.findLeasePathsByHolder(holderID);
    }

    return paths;
  }

  public String getHolder() {
    return holder;
  }

  void replacePath(LeasePath oldpath, LeasePath newpath) {
    getPaths().remove(oldpath);
    getPaths().add(newpath);
  }
}