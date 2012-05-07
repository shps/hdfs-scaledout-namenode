package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Field;
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
  /* List of reader name nodes */
  List<DFSClient> readerNameNodes;
  /* List of writer name nodes */
  List<DFSClient> writerNameNodes;
  private static Log LOG = LogFactory.getLog(NameNodeSelector.class);
  /* Actual Namenode selector logic based on policy */
  private static NameNodeSelector nnSelectorPolicy = null;

  /** Loads the appropriate instance of the the NameNodeSelector from the configuration file
   * So that appropriate reader/ writer namenodes can be selected for each operation
   * @param conf - The configuration from hdfs
   * @param readerNamenodes  - The list of reader Namenodes for read operations
   * @param writerNamenodes - The list of writer Namenodes for write operations
   * @return NameNodeSelector - Returns the implementor of the NameNodeSelector depending on the policy as specified the configuration
   */
  public static NameNodeSelector getInstance(Configuration conf, List<DFSClient> readerNamenodes, List<DFSClient> writerNamenodes) {

    // We allow the writer to also do read operations (as it might sit idle)
    readerNamenodes.addAll(writerNamenodes);

    if (nnSelectorPolicy != null) {
      // reset all the reader / writer NNs
      nnSelectorPolicy.readerNameNodes = readerNamenodes;
      nnSelectorPolicy.writerNameNodes = writerNamenodes;
      return nnSelectorPolicy;
    }
    else {
      boolean error = false;


      // Getting appropriate policy
      String className = conf.get(DFSConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, RoundRobinNameNodeSelector.class.getName());

      Object objSelector = null;

      try {
        Class clsSelector = Class.forName(className);
        objSelector = clsSelector.newInstance();

        if (objSelector instanceof NameNodeSelector) {

          // Setting the readerNameNodes field at runtime
          Field readerNN = clsSelector.getSuperclass().getDeclaredField("readerNameNodes");
          readerNN.set(objSelector, readerNamenodes);

          // Setting the writerNameNodes field at runtime
          Field writerNN = clsSelector.getSuperclass().getDeclaredField("writerNameNodes");
          writerNN.set(objSelector, writerNamenodes);

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
        policy.readerNameNodes = readerNamenodes;
        policy.writerNameNodes = writerNamenodes;
        return policy;
      }
      else {
        // No errors
        LOG.info("Successfully loaded Namenode selector policy [" + nnSelectorPolicy.getClass().getName() + "]");
        return nnSelectorPolicy;
      }
    }
  }

  /**Gets the appropriate reader namenode for a read operation
   * @return DFSClient
   */
  protected abstract DFSClient getReaderNameNode();

  /**Gets the appropriate writer namenode for a read/write operation
   * @return DFSClient
   */
  protected abstract DFSClient getWriterNameNode();

  /**Gets the appropriate reader namenode for a read operation by policy and retries for next namenode incase of failure
   * @return DFSClient
   */
  public final DFSClient getNextReaderNameNode() throws IOException {

    for (int nnIndex = 1; nnIndex <= readerNameNodes.size(); nnIndex++) {

      // Returns next nn based on policy
      DFSClient client = getReaderNameNode();
      LOG.debug("Next RNN: "+client.getId());

      // [JUDE] We won't use retry here for the same client as there is already an internal retry mechanism in ipc package
      //for (int retryIndex = RETRY_COUNT; retryIndex > 0; retryIndex--) {
        if (client.pingNamenode()) {
          return client;
        }
        else {
          LOG.warn("RNN ["+client.getId()+"] failed. Trying next RNN...");
        }
      //}

      // Switch to next Reader nn
    }

    // At this point, we have tried almost all NNs, all are not reachable. Something is wrong
    throw new IOException("getNextReaderNameNode() :: Unable to connect to any reader / writer Namenode");
  }

  /**Gets the appropriate writer namenode for a read/write operation by policy and retries for next namenode incase of failure
   * @return DFSClient
   */
  public final DFSClient getNextWriterNameNode() throws IOException {

    for (int nnIndex = 1; nnIndex <= writerNameNodes.size(); nnIndex++) {

      // Returns next nn based on policy
      DFSClient client = getWriterNameNode();
      LOG.debug("Next WNN: "+client.getId());

      // [JUDE] We won't use retry here for the same client as there is already an internal retry mechanism in ipc package
      //for (int retryIndex = RETRY_COUNT; retryIndex > 0; retryIndex--) {
        if (client.pingNamenode()) {
          return client;
        }
        else {
          LOG.warn("RNN ["+client.getId()+"] failed. Trying next RNN...");
        }
      //}

      // Switch to next Writer nn
    }

    // At this point, we have tried almost all NNs, all are not reachable. Something is wrong
    throw new IOException("getNextWriterNameNode() :: Unable to connect to any writer Namenode");
  }
   void printReadersWritersNNs() {
    String nns = "Readers: ";
    for(DFSClient client : readerNameNodes) {
      nns += client.getId() +", ";
    }
    
    nns += " Writers: ";
    for(DFSClient client : writerNameNodes) {
      nns += client.getId();
    }
    
    LOG.debug(nns);
  }
   
   public int getTotalCountReaders() {
     return readerNameNodes.size();
   }
   
   public int getTotalCountWriters() {
     return writerNameNodes.size();
   }
  @Override
  public boolean equals(Object o) {
    if (o instanceof NameNodeSelector) {
      // [JUDE] Done just for testing
      if ((((NameNodeSelector) o).readerNameNodes.size() == this.readerNameNodes.size())
          && (((NameNodeSelector) o).writerNameNodes.size() == this.writerNameNodes.size())) {
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
