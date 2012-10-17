package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/****************************************************************
This interface is a policy for selecting the appropriate NameNodes 
 * (either in round robin or random style or any other style depending on implementation)
 * to perform hdfs read/write operations
 * 
 * In case we are not able to load the class provided in the configuration, we load default policy (which is Random selection)
 * 
 * This is done in order to support mulitple reader/ writer name nodes but offer load balanced requests among them
 *
 *****************************************************************/
public abstract class NameNodeSelector {

  /* Retry counter for retrying next namenode incase of failure*/
  static final int RETRY_COUNT = 3;
  /* List of name nodes */
  protected List<DFSClient> namenodes;
  /* Namenodes that did not respond to ping messages. Hence might have crashed */
  private List<DFSClient> inactiveNamenodes = new ArrayList<DFSClient>();
  private static Log LOG = LogFactory.getLog(NameNodeSelector.class);

  /** Loads the appropriate instance of the the NameNodeSelector from the configuration file
   * So that appropriate reader/ writer namenodes can be selected for each operation
   * @param conf - The configuration from hdfs
   * @param namenodes - The list of namenodes for read/write operations
   * @return NameNodeSelector - Returns the implementor of the NameNodeSelector depending on the policy as specified the configuration
   */
  public static NameNodeSelector createInstance(Configuration conf, List<DFSClient> nns) {

    NameNodeSelector nnSelectorPolicy = null;

    boolean error = false;


    // Getting appropriate policy
    String className = conf.get(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, RoundRobinNameNodeSelector.class.getName());

    Object objSelector = null;

    try {
      Class clsSelector = Class.forName(className);
      objSelector = clsSelector.newInstance();

      if (objSelector instanceof NameNodeSelector) {

        // Setting the 'namenodes' field at runtime
        Field field = clsSelector.getSuperclass().getDeclaredField("namenodes");
        field.set(objSelector, nns);

        nnSelectorPolicy = (NameNodeSelector) objSelector;

      }
      else {
        LOG.warn("getInstance() :: Invalid class type provided for name node selector policy. Not an instance of abstract class NameNodeSelector [class-name: " + className + "]");
        error = true;
      }
    }
    catch (ClassNotFoundException ex) {
      LOG.warn("getInstance() :: Invalid class name provided for name node selector policy [class-name: " + className + "]", ex);
      error = true;
    }
    catch (InstantiationException ex) {
      LOG.warn("getInstance() :: Unable to instiniate class provided for name node selector policy [class-name: " + className + "]", ex);
      error = true;
    }
    catch (IllegalAccessException ex) {
      LOG.warn("getInstance() :: Unable to access class provided for name node selector policy [class-name: " + className + "]", ex);
      error = true;
    }
    catch (NoSuchFieldException ex) {
      LOG.warn("getInstance() :: Unable to set fields for class provided for name node selector policy [class-name: " + className + "]", ex);
      error = true;
    }

    if (error) {
      // In case of error, get default name node selector policy
      LOG.info("Selecting default Namenode selection policy");
      RoundRobinNameNodeSelector policy = new RoundRobinNameNodeSelector();
      policy.namenodes = nns;
      return policy;
    }
    else {
      // No errors
      LOG.info("Successfully loaded Namenode selector policy [" + nnSelectorPolicy.getClass().getName() + "]");
      return nnSelectorPolicy;
    }
  }

  /**Gets the appropriate namenode for a read/write operation
   * @return DFSClient
   */
  protected abstract DFSClient getNamenode();

  /**Gets the appropriate writer namenode for a read/write operation by policy and retries for next namenode incase of failure
   * @return DFSClient
   */
  public DFSClient getNextNamenode() throws IOException {

    for (int nnIndex = 1; nnIndex <= namenodes.size(); nnIndex++) {

      // Returns next nn based on policy
      DFSClient client = getNamenode();
      LOG.info("Next NN: " + client.getId());

      // skip over inactive namenodes
      if (inactiveNamenodes.contains(client)) {
        continue;
      }
      
      // check for connectivity with namenode
      if (client.pingNamenode()) {
        return client;
      }
      else {
        inactiveNamenodes.add(client);
        LOG.warn("NN [" + client.getId() + "] failed. Trying next NN...");
      }
      // Switch to next Writer nn
    }

    // At this point, we have tried almost all NNs, all are not reachable. Something is wrong
    throw new IOException("getNextNamenode() :: Unable to connect to any Namenode");
  }

  void printNamenodes() {
    String nns = "namenodes: ";
    for (DFSClient client : namenodes) {
      nns += client.getId() + ", ";
    }
    LOG.debug(nns);
  }

  public int getTotalNamenodes() {
    return namenodes.size();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof NameNodeSelector) {
      // [JUDE] Done just for testing
      if ((((NameNodeSelector) o).namenodes.size() == this.namenodes.size())) {
        return true;
      }
      else {
        return false;
      }
    }
    else {
      return false;
    }
  }
}
