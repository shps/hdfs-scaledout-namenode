/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.logging.Level;
import java.util.logging.Logger;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_UPGRADE_PERMISSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_UPGRADE_PERMISSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_KEY;
import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockAcquirer;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager.*;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler.*;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

/**
 * *************************************************
 * FSNamesystem does the actual bookkeeping work for the DataNode.
 *
 * It tracks several important tables.
 *
 * 1) valid fsname --> blocklist (kept on disk, logged) 2) Set of all valid
 * blocks (inverted #1) 3) block --> machinelist (kept in memory, rebuilt
 * dynamically from reports) 4) machine --> blocklist (inverted #2) 5) LRU cache
 * of updated-heartbeat machines
 * *************************************************
 */
@InterfaceAudience.Private
@Metrics(context = "dfs")
public class FSNamesystem implements Namesystem, FSClusterStats,
        FSNamesystemMBean, NameNodeMXBean {

  static final Log LOG = LogFactory.getLog(FSNamesystem.class);
  private static final ThreadLocal<StringBuilder> auditBuffer =
          new ThreadLocal<StringBuilder>() {

            protected StringBuilder initialValue() {
              return new StringBuilder();
            }
          };

  private static final void logAuditEvent(UserGroupInformation ugi,
          InetAddress addr, String cmd, String src, String dst,
          HdfsFileStatus stat) {
    final StringBuilder sb = auditBuffer.get();
    sb.setLength(0);
    sb.append("ugi=").append(ugi).append("\t");
    sb.append("ip=").append(addr).append("\t");
    sb.append("cmd=").append(cmd).append("\t");
    sb.append("src=").append(src).append("\t");
    sb.append("dst=").append(dst).append("\t");
    if (null == stat) {
      sb.append("perm=null");
    } else {
      sb.append("perm=");
      sb.append(stat.getOwner()).append(":");
      sb.append(stat.getGroup()).append(":");
      sb.append(stat.getPermission());
    }
    auditLog.info(sb);
  }
  /**
   * Logger for audit events, noting successful FSNamesystem operations. Emits
   * to FSNamesystem.audit at INFO. Each event causes a set of tab-separated
   * <code>key=value</code> pairs to be written for the following properties:
   * <code>
   * ugi=&lt;ugi in RPC&gt;
   * ip=&lt;remote IP&gt;
   * cmd=&lt;command&gt;
   * src=&lt;src path&gt;
   * dst=&lt;dst path (optional)&gt;
   * perm=&lt;permissions (optional)&gt;
   * </code>
   */
  public static final Log auditLog = LogFactory.getLog(
          FSNamesystem.class.getName() + ".audit");
  static final int DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED = 100;
  static int BLOCK_DELETION_INCREMENT = 1000;
  private boolean isPermissionEnabled;
  private UserGroupInformation fsOwner;
  private String supergroup;
  private PermissionStatus defaultPermission;
  // FSNamesystemMetrics counter variables
  @Metric
  private MutableCounterInt expiredHeartbeats;
  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
          TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  private DelegationTokenSecretManager dtSecretManager;
  //
  // Stores the correct file name hierarchy
  //
  FSDirectory dir;
  private BlockManager blockManager;
  private DatanodeStatistics datanodeStatistics;
  // Block pool ID used by this namenode
  private String blockPoolId = "h4ck3d-810ck-p001";
  LeaseManager leaseManager = new LeaseManager(this);
  Daemon lmthread = null;   // LeaseMonitor thread
  Daemon smmthread = null;  // SafeModeMonitor thread
  //TODO:kamal resource monitor
//  Daemon nnrmthread = null; // NamenodeResourceMonitor thread
//TODO:kamal resource monitor
//  private volatile boolean hasResourcesAvailable = false;
  private volatile boolean fsRunning = true;
  long systemStart = 0;
  //TODO:kamal resource monitor
  //resourceRecheckInterval is how often namenode checks for the disk space availability
//  private long resourceRecheckInterval;
  //TODO:kamal resource monitor
  // The actual resource checker instance.
//  NameNodeResourceChecker nnResourceChecker;
  private FsServerDefaults serverDefaults;
  // allow appending to hdfs files
  private boolean supportAppends = true;
  private ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure =
          ReplaceDatanodeOnFailure.DEFAULT;
  private volatile SafeModeInfo safeMode;  // safe mode information
  private long maxFsObjects = 0;          // maximum number of fs objects
  /**
   * The global generation stamp for this file system.
   */
  private final GenerationStamp generationStamp = new GenerationStamp();
  // precision of access times.
  private long accessTimePrecision = 0;
  // lock to protect FSNamesystem.
  private ReentrantReadWriteLock fsLock;
  private NameNode nameNode;
  private static boolean systemLevelLockEnabled = false;
  private static boolean rowLevelLockEnabled = false;

  /**
   * FSNamesystem constructor.
   */
  FSNamesystem(Configuration conf, NameNode nameNode) throws IOException {
    try {
      this.nameNode = nameNode;
      initialize(conf, null);
    } catch (IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    }
  }

  /**
   * Initialize FSNamesystem.
   */
  private void initialize(Configuration conf, FSImage fsImage)
          throws IOException {
//TODO:kamal resource monitor
//      resourceRecheckInterval = conf.getLong(
//        DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
//        DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);
//    nnResourceChecker = new NameNodeResourceChecker(conf);
//    checkAvailableResources();
    systemLevelLockEnabled = conf.getBoolean(DFSConfigKeys.DFS_SYSTEM_LEVEL_LOCK_ENABLED_KEY, DFSConfigKeys.DFS_SYSTEM_LEVEL_LOCK_ENABLED_DEFAULT);
    rowLevelLockEnabled = conf.getBoolean(DFSConfigKeys.DFS_ROW_LEVEL_LOCK_ENABLED_KEY, DFSConfigKeys.DFS_ROW_LEVEL_LOCK_ENABLED_DEFAULT);
    StorageFactory.setConfiguration(conf);
    LOG.info(DFSConfigKeys.DFS_SYSTEM_LEVEL_LOCK_ENABLED_KEY + " = " + systemLevelLockEnabled);
    LOG.info(DFSConfigKeys.DFS_ROW_LEVEL_LOCK_ENABLED_KEY + " = " + rowLevelLockEnabled);
    LOG.info("DFS_INODE_CACHE_ENABLED=" + DFSConfigKeys.DFS_INODE_CACHE_ENABLED);
    this.systemStart = now();
    this.blockManager = new BlockManager(this, conf);
    this.fsLock = new ReentrantReadWriteLock(true); // fair locking
    setConfigurationParameters(conf);
    dtSecretManager = createDelegationTokenSecretManager(conf);

    if (NameNode.getStartupOption(conf) != StartupOption.FORMAT) {
      if (isWritingNN()) {
        this.registerMBean(); // register the MBean for the FSNamesystemState
        this.datanodeStatistics = blockManager.getDatanodeManager().getDatanodeStatistics();
      }
    }
    //TODO: truncate the DB tables when StartupOption.FORMAT
    if (fsImage == null) {
      this.dir = new FSDirectory(this, conf);
      StartupOption startOpt = NameNode.getStartupOption(conf);
      this.dir.loadFSImage(startOpt);
      long timeTakenToLoadFSImage = now() - systemStart;
      LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
      NameNode.getNameNodeMetrics().setFsImageLoadTime(
              (int) timeTakenToLoadFSImage);
    } else {
      this.dir = new FSDirectory(fsImage, this, conf);

    }

    TransactionalRequestHandler initHandler = new TransactionalRequestHandler(OperationType.INITIALIZE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        INode rootInode = EntityManager.find(INode.Finder.ByNameAndParentId, INodeDirectory.ROOT_NAME, -1L);
        if (rootInode == null) {
          dir.rootDir.setId(0);
          dir.rootDir.setParentId(-1);
          EntityManager.add(dir.rootDir);
        } else {
          dir.rootDir = (INodeDirectoryWithQuota) rootInode;
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockAcquirer.acquireLock(LockType.READ, INode.Finder.ByNameAndParentId, INodeDirectory.ROOT_NAME, -1L);
      }
    };
    initHandler.handle();

    if (DFSConfigKeys.DFS_INODE_CACHE_ENABLED) {
      INodeCache cache = INodeCacheImpl.getInstance(); //added for magic cache
      cache.putRoot(this.dir.rootDir); //added for magic cache
    }

    this.safeMode = new SafeModeInfo(conf);
  }

  public static boolean systemLevelLock() {
    return systemLevelLockEnabled;
  }
  
  public static boolean rowLevelLock()
  {
    return rowLevelLockEnabled;
  }

  void activateSecretManager() throws IOException {
    if (dtSecretManager != null) {
      dtSecretManager.startThreads();
    }
  }

  @Override
  public boolean isWritingNN() {
    return nameNode == null ? false : nameNode.isWritingNN(); //FIXME: find out a better way of handling namenode format
  }

  /**
   * Activate FSNamesystem daemons.
   */
  void activate(final Configuration conf) throws IOException, StorageException, PersistanceException {

    if (isWritingNN()) {
      writeLock();
      try {
        EntityManager.begin();
        setBlockTotal();
        EntityManager.commit();
        blockManager.activate(conf);
        lmthread = new Daemon(leaseManager.new Monitor());
        lmthread.start();
        registerMXBean();
      } finally {
        writeUnlock();
      }
    }
//    TransactionalRequestHandler activateHandler = new TransactionalRequestHandler(OperationType.ACTIVATE) {
//
//      @Override
//      public Object performTask() throws PersistanceException, IOException {
//        if (isWritingNN()) {
//          setBlockTotal();
//          blockManager.activate(conf);
//          lmthread = new Daemon(leaseManager.new Monitor());
//          lmthread.start();
//          registerMXBean();
//        }
////      TODO:kamal, resouce monitor
////      this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
////      nnrmthread.start();
//        return null;
//      }
//
//      @Override
//      public void acquireLock() throws PersistanceException, IOException {
//        // FIXME
//      }
//    };
//    activateHandler.handleWithWriteLock(this);
    DefaultMetricsSystem.instance().register(this);
  }

  void activateOld(Configuration conf) throws IOException, PersistanceException {
    writeLock();
    try {

      if (isWritingNN()) {
        setBlockTotal();
        blockManager.activate(conf);
        this.lmthread = new Daemon(leaseManager.new Monitor());
        lmthread.start();
      }
//      TODO:kamal, resouce monitor
//      this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
//      nnrmthread.start();
    } finally {
      writeUnlock();
    }

    if (isWritingNN()) {
      registerMXBean();
    }
    DefaultMetricsSystem.instance().register(this);
  }

  public static Collection<URI> getNamespaceDirs(Configuration conf) {
    return getStorageDirs(conf, DFS_NAMENODE_NAME_DIR_KEY);
  }

  private static Collection<URI> getStorageDirs(Configuration conf,
          String propertyName) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(propertyName);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if (startOpt == StartupOption.IMPORT) {
      // In case of IMPORT this will get rid of default directories 
      // but will retain directories specified in hdfs-site.xml
      // When importing image from a checkpoint, the name-node can
      // start with empty set of storage directories.
      Configuration cE = new HdfsConfiguration(false);
      cE.addResource("core-default.xml");
      cE.addResource("core-site.xml");
      cE.addResource("hdfs-default.xml");
      Collection<String> dirNames2 = cE.getTrimmedStringCollection(propertyName);
      dirNames.removeAll(dirNames2);
      if (dirNames.isEmpty()) {
        LOG.warn("!!! WARNING !!!"
                + "\n\tThe NameNode currently runs without persistent storage."
                + "\n\tAny changes to the file system meta-data may be lost."
                + "\n\tRecommended actions:"
                + "\n\t\t- shutdown and restart NameNode with configured \""
                + propertyName + "\" in hdfs-site.xml;"
                + "\n\t\t- use Backup Node as a persistent and up-to-date storage "
                + "of the file system meta-data.");
      }
    } else if (dirNames.isEmpty()) {
      dirNames = Collections.singletonList("file:///tmp/hadoop/dfs/name");
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  public static Collection<URI> getNamespaceEditsDirs(Configuration conf) {
    return getStorageDirs(conf, DFS_NAMENODE_EDITS_DIR_KEY);
  }

  @Override
  public void readLock() {
    if (systemLevelLock()) {
      this.fsLock.readLock().lock();
    }
  }

  @Override
  public void readUnlock() {
    if (systemLevelLock()) {
      this.fsLock.readLock().unlock();
    }
  }

  @Override
  public void writeLock() {
    if (systemLevelLock()) {
      this.fsLock.writeLock().lock();
    }
  }

  @Override
  public void writeUnlock() {
    if (systemLevelLock()) {
      this.fsLock.writeLock().unlock();
    }
  }

  @Override
  public boolean hasWriteLock() {
    if (!systemLevelLock()) {
      return true;
    }
    return this.fsLock.isWriteLockedByCurrentThread();
  }

  @Override
  public boolean hasReadLock() {
    if (!systemLevelLock()) {
      return true;
    }
    return this.fsLock.getReadHoldCount() > 0;
  }

  @Override
  public boolean hasReadOrWriteLock() {
    if (!systemLevelLock()) {
      return true;
    }
    return hasReadLock() || hasWriteLock();
  }

  /**
   * dirs is a list of directories where the filesystem directory state is
   * stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
    this.fsLock = new ReentrantReadWriteLock(true);
    this.blockManager = new BlockManager(this, conf);
    setConfigurationParameters(conf);
    this.dir = new FSDirectory(fsImage, this, conf);
    dtSecretManager = createDelegationTokenSecretManager(conf);
  }

  /**
   * Initializes some of the members from configuration
   */
  private void setConfigurationParameters(Configuration conf)
          throws IOException {
    fsOwner = UserGroupInformation.getCurrentUser();

    LOG.info("fsOwner=" + fsOwner);

    this.supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
            DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
            DFS_PERMISSIONS_ENABLED_DEFAULT);
    LOG.info("supergroup=" + supergroup);
    LOG.info("isPermissionEnabled=" + isPermissionEnabled);
    short filePermission = (short) conf.getInt(DFS_NAMENODE_UPGRADE_PERMISSION_KEY,
            DFS_NAMENODE_UPGRADE_PERMISSION_DEFAULT);
    this.defaultPermission = PermissionStatus.createImmutable(
            fsOwner.getShortUserName(), supergroup, new FsPermission(filePermission));

    this.serverDefaults = new FsServerDefaults(
            conf.getLong(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
            conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
            conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
            (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
            conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT));

    this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY,
            DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

    this.accessTimePrecision = conf.getLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 0);
    this.supportAppends = conf.getBoolean(DFS_SUPPORT_APPEND_KEY,
            DFS_SUPPORT_APPEND_DEFAULT);

    this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
  }

  /**
   * Return the default path permission when upgrading from releases with no
   * permissions (<=0.15) to releases with permissions (>=0.16)
   */
  protected PermissionStatus getUpgradePermission() {
    return defaultPermission;
  }

  NamespaceInfo getNamespaceInfo() {
    readLock();
    try {
      // Hack time
      return new NamespaceInfo(dir.fsImage.getStorage().getNamespaceID(),
              getClusterId(), getBlockPoolId(),
              dir.fsImage.getStorage().getCTime(),
              upgradeManager.getUpgradeVersion());
    } finally {
      readUnlock();
    }
  }

  /**
   * Close down this file system manager. Causes heartbeat and lease daemons to
   * stop; waits briefly for them to finish, but a short timeout returns control
   * back to caller.
   */
  void close() {
    fsRunning = false;
    try {
      if (blockManager != null) {
        blockManager.close();
      }
      if (smmthread != null) {
        smmthread.interrupt();
      }
      if (dtSecretManager != null) {
        dtSecretManager.stopThreads();
      }
//TODO:kamal resource monitor
//      if (nnrmthread != null) nnrmthread.interrupt();
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
        if (dir != null) {
          dir.close();
        }
      } catch (InterruptedException ie) {
      } catch (IOException ie) {
        LOG.error("Error closing FSDirectory", ie);
        IOUtils.cleanup(LOG, dir);
      }
    }
  }

  @Override
  public boolean isRunning() {
    return fsRunning;
  }

  /**
   * Dump all metadata into specified file
   */
  void metaSave(final String filename) throws IOException {
    TransactionalRequestHandler metasaveHandler = new TransactionalRequestHandler(OperationType.META_SAVE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        checkSuperuserPrivilege();
        File file = new File(System.getProperty("hadoop.log.dir"), filename);
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file,
                true)));

        long totalInodes = dir.totalInodes();
        long totalBlocks = getBlocksTotal();
        out.println(totalInodes + " files and directories, " + totalBlocks
                + " blocks = " + (totalInodes + totalBlocks) + " total");

        final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
        final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
        blockManager.getDatanodeManager().fetchDatanodes(live, dead, false);
        out.println("Live Datanodes: " + live.size());
        out.println("Dead Datanodes: " + dead.size());
        blockManager.metaSave(out);

        out.flush();
        out.close();
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
    metasaveHandler.handleWithWriteLock(this);
  }

  long getDefaultBlockSize() {
    return serverDefaults.getBlockSize();
  }

  FsServerDefaults getServerDefaults() {
    return serverDefaults;
  }

  long getAccessTimePrecision() {
    return accessTimePrecision;
  }

  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   *
   * @throws IOException
   */
  void setPermission(final String src, final FsPermission permission)
          throws AccessControlException, FileNotFoundException, SafeModeException,
          UnresolvedLinkException, IOException {
    TransactionalRequestHandler setPermissionHanlder = new TransactionalRequestHandler(OperationType.SET_PERMISSION) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        HdfsFileStatus resultingStat = null;
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot set permission for " + src, safeMode);
        }
        checkOwner(src);
        dir.setPermission(src, permission);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          resultingStat = dir.getFileInfo(src, false);
        }

        //getEditLog().logSync();
        if (auditLog.isInfoEnabled()
                && isExternalInvocation()) {
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                  Server.getRemoteIp(),
                  "setPermission", src, null, resultingStat);
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager lm = new TransactionLockManager();
        lm.addINode(INodeResolveType.ONLY_PATH,
                INodeLockType.WRITE,
                new String[]{src}, getFsDirectory().getRootDir()).
                addBlock(LockType.READ).
                acquire();
      }
    };
    setPermissionHanlder.handleWithWriteLock(this);
  }

  /**
   * Set owner for an existing file.
   *
   * @throws IOException
   */
  void setOwner(final String src, final String username, final String group)
          throws AccessControlException, FileNotFoundException, SafeModeException,
          UnresolvedLinkException, IOException {
    TransactionalRequestHandler setOwnerHandler = new TransactionalRequestHandler(OperationType.SET_OWNER) {

      @Override
      public Object performTask() throws PersistanceException, IOException {

        HdfsFileStatus resultingStat = null;
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot set owner for " + src, safeMode);
        }
        FSPermissionChecker pc = checkOwner(src);
        if (!pc.isSuper) {
          if (username != null && !pc.user.equals(username)) {
            throw new AccessControlException("Non-super user cannot change owner.");
          }
          if (group != null && !pc.containsGroup(group)) {
            throw new AccessControlException("User does not belong to " + group
                    + " .");
          }
        }

        dir.setOwner(src, username, group);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          resultingStat = dir.getFileInfo(src, false);
        }

        //getEditLog().logSync();
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                  Server.getRemoteIp(),
                  "setOwner", src, null, resultingStat);
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager lm = new TransactionLockManager();
        lm.addINode(INodeResolveType.ONLY_PATH,
                INodeLockType.WRITE,
                new String[]{src}, getFsDirectory().getRootDir()).
                addBlock(LockType.READ).
                acquire();
      }
    };
    setOwnerHandler.handleWithWriteLock(this);
  }

  /**
   * Get block locations within the specified range.
   *
   * @see ClientProtocol#getBlockLocations(String, long, long)
   * @throws FileNotFoundException, UnresolvedLinkException, IOException
   */
  LocatedBlocks getBlockLocations(final String src, final long offset, final long length,
          final boolean doAccessTime, final boolean needBlockToken) throws FileNotFoundException,
          UnresolvedLinkException, IOException {
    TransactionalRequestHandler getBlockLocationsHandler = new TransactionalRequestHandler(OperationType.GET_BLOCK_LOCATIONS) {

      @Override
      public Object performTask() throws PersistanceException, IOException {

        if (isPermissionEnabled) {
          checkPathAccess(src, FsAction.READ);
        }

        if (offset < 0) {
          throw new HadoopIllegalArgumentException(
                  "Negative offset is not supported. File: " + src);
        }
        if (length < 0) {
          throw new HadoopIllegalArgumentException(
                  "Negative length is not supported. File: " + src);
        }
        final LocatedBlocks ret = getBlockLocationsUpdateTimes(src,
                offset, length, doAccessTime, needBlockToken);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                  Server.getRemoteIp(),
                  "open", src, null, null);
        }
        return ret;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager lm = new TransactionLockManager();
        lm.addINode(INodeResolveType.ONLY_PATH, INodeLockType.WRITE, new String[]{src}, getFsDirectory().getRootDir());
        lm.addBlock(LockType.READ).
                addReplica(LockType.READ).
                addExcess(LockType.READ).
                addCorrupt(LockType.READ).
                addReplicaUc(LockType.READ);
        lm.acquire();
      }
    };
    return (LocatedBlocks) getBlockLocationsHandler.handle();
  }

  /*
   * Get block locations within the specified range, updating the access times
   * if necessary.
   */
  private LocatedBlocks getBlockLocationsUpdateTimes(String src,
          long offset,
          long length,
          boolean doAccessTime,
          boolean needBlockToken)
          throws FileNotFoundException, UnresolvedLinkException, IOException, PersistanceException {

    //for (int attempt = 0; attempt < 2; attempt++) {
    // if (attempt == 0) { // first attempt is with readlock
    //    readLock();
    //  }  else { // second attempt is with  write lock
    //    writeLock(); // writelock is needed to set accesstime
    //  }
    // if the namenode is in safemode, then do not update access time
    if (isInSafeMode()) {
      doAccessTime = false;
    }

    //try {
    long now = now();
    INodeFile inode = dir.getFileINode(src);
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    assert !inode.isLink();
    if (doAccessTime && isAccessTimeSupported()) {
      if (now <= inode.getAccessTime() + getAccessTimePrecision()) {
        // if we have to set access time but we only have the readlock, then
        // restart this entire operation with the writeLock.
        //if (attempt == 0) {
        //  continue;
        //}
      }
      dir.setTimes(src, inode, -1, now, false);
    }
    return blockManager.createLocatedBlocks(inode.getBlocks(),
            inode.computeFileSize(false),
            inode.isUnderConstruction(),
            offset, length, needBlockToken);
    //} finally {
    //  if (attempt == 0) {
    //    readUnlock();
    //  } else {
    //   writeUnlock();
    //  }
    // }
    //}
    //return null; // can never reach here
  }

  /**
   * Moves all the blocks from srcs and appends them to trg To avoid rollbacks
   * we will verify validitity of ALL of the args before we start actual move.
   *
   * @param target
   * @param srcs
   * @throws IOException
   */
  void concat(final String target, final String[] srcs)
          throws IOException, UnresolvedLinkException {
    if (FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs)
              + " to " + target);
    }

    // verify args
    if (target.isEmpty()) {
      throw new IllegalArgumentException("Target file name is empty");
    }
    if (srcs == null || srcs.length == 0) {
      throw new IllegalArgumentException("No sources given");
    }

    // We require all files be in the same directory
    String trgParent =
            target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
    for (String s : srcs) {
      String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
      if (!srcParent.equals(trgParent)) {
        throw new IllegalArgumentException(
                "Sources and target are not in the same directory");
      }
    }

    TransactionalRequestHandler concatHandler = new TransactionalRequestHandler(OperationType.CONCAT) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot concat " + target, safeMode);
        }
        concatInternal(target, srcs);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          return dir.getFileInfo(target, false);
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        String[] paths = new String[srcs.length + 1];
        System.arraycopy(srcs, 0, paths, 0, srcs.length);
        paths[srcs.length] = target;
        TransactionLockManager lm = new TransactionLockManager();
        lm.addINode(INodeResolveType.ONLY_PATH, INodeLockType.WRITE_ON_PARENT, paths, getFsDirectory().getRootDir());
        lm.addBlock(LockType.WRITE);
        lm.acquire();
      }
    };
    HdfsFileStatus resultingStat = (HdfsFileStatus) concatHandler.handleWithWriteLock(this);
    //getEditLog().logSync();
    if (auditLog.isInfoEnabled()
            && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getLoginUser(),
              Server.getRemoteIp(),
              "concat", Arrays.toString(srcs), target, resultingStat);
    }
  }

  /**
   * See {@link #concat(String, String[])}
   */
  private void concatInternal(String target, String[] srcs)
          throws IOException, UnresolvedLinkException, PersistanceException {
    assert hasWriteLock();
    // write permission for the target
    if (isPermissionEnabled) {
      checkPathAccess(target, FsAction.WRITE);

      // and srcs
      for (String aSrc : srcs) {
        checkPathAccess(aSrc, FsAction.READ); // read the file
        checkParentAccess(aSrc, FsAction.WRITE); // for delete 
      }
    }

    // to make sure no two files are the same
    Set<INode> si = new HashSet<INode>();

    // we put the following prerequisite for the operation
    // replication and blocks sizes should be the same for ALL the blocks
    // check the target
    INode inode = dir.getFileINode(target);

    if (inode == null) {
      throw new IllegalArgumentException("concat: trg file doesn't exist");
    }
    if (inode.isUnderConstruction()) {
      throw new IllegalArgumentException("concat: trg file is uner construction");
    }

    INodeFile trgInode = (INodeFile) inode;

    // per design trg shouldn't be empty and all the blocks same size
    if (trgInode.getBlocks().isEmpty()) {
      throw new IllegalArgumentException("concat: " + target + " file is empty");
    }

    long blockSize = trgInode.getPreferredBlockSize();

    // check the end block to be full
    if (blockSize != trgInode.getBlocks().get(trgInode.getBlocks().size() - 1).getNumBytes()) {
      throw new IllegalArgumentException(target + " blocks size should be the same");
    }

    si.add(trgInode);
    short repl = trgInode.getReplication();

    // now check the srcs
    boolean endSrc = false; // final src file doesn't have to have full end block
    for (int i = 0; i < srcs.length; i++) {
      String src = srcs[i];
      if (i == srcs.length - 1) {
        endSrc = true;
      }

      INodeFile srcInode = dir.getFileINode(src);

      if (src.isEmpty()
              || srcInode == null
              || srcInode.isUnderConstruction()
              || srcInode.getBlocks().isEmpty()) {
        throw new IllegalArgumentException("concat: file " + src
                + " is invalid or empty or underConstruction");
      }

      // check replication and blocks size
      if (repl != srcInode.getReplication()) {
        throw new IllegalArgumentException(src + " and " + target + " "
                + "should have same replication: "
                + repl + " vs. " + srcInode.getReplication());
      }

      //boolean endBlock=false;
      // verify that all the blocks are of the same length as target
      // should be enough to check the end blocks
      int idx = srcInode.getBlocks().size() - 1;
      if (endSrc) {
        idx = srcInode.getBlocks().size() - 2; // end block of endSrc is OK not to be full
      }
      if (idx >= 0 && srcInode.getBlocks().get(idx).getNumBytes() != blockSize) {
        throw new IllegalArgumentException("concat: blocks sizes of "
                + src + " and " + target + " should all be the same");
      }

      si.add(srcInode);
    }

    // make sure no two files are the same
    if (si.size() < srcs.length + 1) { // trg + srcs
      // it means at least two files are the same
      throw new IllegalArgumentException("at least two files are the same");
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.concat: "
              + Arrays.toString(srcs) + " to " + target);
    }

    dir.concat(target, srcs);
  }

  /**
   * stores the modification and access time for this inode. The access time is
   * precise upto an hour. The transaction, if needed, is written to the edits
   * log but is not flushed.
   */
  void setTimes(final String src, final long mtime, final long atime)
          throws IOException, UnresolvedLinkException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. "
              + " Please set dfs.support.accessTime configuration parameter.");
    }
    TransactionalRequestHandler setTimesHanlder = new TransactionalRequestHandler(OperationType.SET_TIMES) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isPermissionEnabled) {
          checkPathAccess(src, FsAction.WRITE);
        }
        INodeFile inode = dir.getFileINode(src);
        if (inode != null) {
          dir.setTimes(src, inode, mtime, atime, true);
          if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            final HdfsFileStatus stat = dir.getFileInfo(src, false);
            logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setTimes", src, null, stat);
          }
        } else {
          throw new FileNotFoundException("File " + src + " does not exist.");
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager lm = new TransactionLockManager();
        lm.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{src}, getFsDirectory().getRootDir()).
                addBlock(TransactionLockManager.LockType.READ).
                acquire();
      }
    };
    setTimesHanlder.handleWithWriteLock(this);
  }

  /**
   * Create a symbolic link.
   */
  void createSymlink(final String target, final String link,
          final PermissionStatus dirPerms, final boolean createParent)
          throws IOException, UnresolvedLinkException {
    TransactionalRequestHandler createSymLinkHandler = new TransactionalRequestHandler(OperationType.CREATE_SYM_LINK) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (!createParent) {
          verifyParentDir(link);
        }
        createSymlinkInternal(target, link, dirPerms, createParent, true);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          return dir.getFileInfo(link, false);
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH_WITH_UNKNOWN_HEAD,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{link},
                getFsDirectory().getRootDir()).
                acquire();
      }
    };
    HdfsFileStatus resultingStat = (HdfsFileStatus) createSymLinkHandler.handleWithWriteLock(this);
    //getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
              Server.getRemoteIp(),
              "createSymlink", link, target, resultingStat);
    }
  }

  /**
   * Create a symbolic link.
   */
  private void createSymlinkInternal(String target, String link,
          PermissionStatus dirPerms, boolean createParent, boolean transactional)
          throws IOException, UnresolvedLinkException, PersistanceException {
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target="
              + target + " link=" + link);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create symlink " + link, safeMode);
    }
    if (!DFSUtil.isValidName(link)) {
      throw new InvalidPathException("Invalid file name: " + link);
    }
    if (!dir.isValidToCreate(link)) {
      throw new IOException("failed to create link " + link
              + " either because the filename is invalid or the file exists");
    }
    if (isPermissionEnabled) {
      checkAncestorAccess(link, FsAction.WRITE);
    }
    // validate that we have enough inodes.
    checkFsObjectLimit(OperationType.CREATE_SYM_LINK);

    // add symbolic link to namespace
    dir.addSymlink(link, target, dirPerms, createParent);
  }

  /**
   * Set replication for an existing file.
   *
   * The NameNode sets new replication and schedules either replication of
   * under-replicated data blocks or removal of the excessive block copies if
   * the blocks are over-replicated.
   *
   * @see ClientProtocol#setReplication(String, short)
   * @param src file name
   * @param replication new replication
   * @return true if successful; false if file does not exist or is a directory
   */
  boolean setReplication(final String src, final short replication) throws IOException {
    blockManager.verifyReplication(src, replication, null);
    TransactionalRequestHandler setReplicationHandler = new TransactionalRequestHandler(OperationType.SET_REPLICATION) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        boolean isFile = false;
        if (isPermissionEnabled) {
          checkPathAccess(src, FsAction.WRITE);
        }

        final short[] oldReplication = new short[1];
        final List<BlockInfo> blocks = dir.setReplication(src, replication, oldReplication);
        isFile = blocks != null;
        if (isFile) {
          blockManager.setReplication(oldReplication[0], replication, src, blocks);
        }
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot set replication for " + src, safeMode);
        }
        //getEditLog().logSync();
        if (isFile && auditLog.isInfoEnabled() && isExternalInvocation()) {
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                  Server.getRemoteIp(),
                  "setReplication", src, null, null);
        }
        return isFile;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
                new String[]{src}, getFsDirectory().getRootDir()).
                addBlock(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE).
                acquire();
      }
    };
    return (Boolean) setReplicationHandler.handleWithWriteLock(this);
  }

  /**
   * Set replication for an existing file.
   *
   * The NameNode sets new replication and schedules either replication of
   * under-replicated data blocks or removal of the excessive block copies if
   * the blocks are over-replicated.
   *
   * @see ClientProtocol#setReplication(String, short)
   * @param src file name
   * @param replication new replication
   * @return true if successful; false if file does not exist or is a directory
   */
  boolean setReplicationOld(final String src, final short replication) throws IOException, PersistanceException {
    blockManager.verifyReplication(src, replication, null);

    final boolean isFile;
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set replication for " + src, safeMode);
      }
      if (isPermissionEnabled) {
        checkPathAccess(src, FsAction.WRITE);
      }

      final short[] oldReplication = new short[1];
      final List<BlockInfo> blocks = dir.setReplication(src, replication, oldReplication);
      isFile = blocks != null;
      if (isFile) {
        blockManager.setReplication(oldReplication[0], replication, src, blocks);
      }
    } finally {
      writeUnlock();
    }

    //getEditLog().logSync();
    if (isFile && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
              Server.getRemoteIp(),
              "setReplication", src, null, null);
    }
    return isFile;
  }

  long getPreferredBlockSize(final String filename)
          throws IOException, UnresolvedLinkException {
    TransactionalRequestHandler getPreferredBlockSizeHandler = new TransactionalRequestHandler(OperationType.GET_PREFERRED_BLOCK_SIZE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isPermissionEnabled) {
          checkTraverse(filename);
        }
        return dir.getPreferredBlockSize(filename);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.READ,
                new String[]{filename}, getFsDirectory().getRootDir()).
                acquire();
      }
    };
    return (Long) getPreferredBlockSizeHandler.handleWithReadLock(this);
  }
  /*
   * Verify that parent directory of src exists.
   */

  private void verifyParentDir(String src) throws FileNotFoundException,
          ParentNotDirectoryException, UnresolvedLinkException, PersistanceException {
    assert hasReadOrWriteLock();
    Path parent = new Path(src).getParent();
    if (parent != null) {
      INode[] pathINodes = dir.getExistingPathINodes(parent.toString());
      INode parentNode = pathINodes[pathINodes.length - 1];
      if (parentNode == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
                + parent.toString());
      } else if (!parentNode.isDirectory() && !parentNode.isLink()) {
        throw new ParentNotDirectoryException("Parent path is not a directory: "
                + parent.toString());
      }
    }
  }

  /**
   * Create a new file entry in the namespace.
   *
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create()}
   */
  void startFile(final String src, final PermissionStatus permissions, final String holder,
          final String clientMachine, final EnumSet<CreateFlag> flag, final boolean createParent,
          final short replication, final long blockSize) throws AccessControlException,
          SafeModeException, FileAlreadyExistsException, UnresolvedLinkException,
          FileNotFoundException, ParentNotDirectoryException, IOException {
    TransactionalRequestHandler startFileHanlder = new TransactionalRequestHandler(OperationType.START_FILE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        startFileInternal(src, permissions, holder, clientMachine, flag,
                createParent, replication, blockSize);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          // [lock] commented to avoid getBlock for the newly added inode
//          final HdfsFileStatus stat = dir.getFileInfo(src, false);
//          logAuditEvent(UserGroupInformation.getCurrentUser(),
//                  Server.getRemoteIp(),
//                  "create", src, null, stat);
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
       TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH_WITH_UNKNOWN_HEAD,
                TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
                new String[]{src},
                getFsDirectory().getRootDir()).
                addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE, holder).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.WRITE).
                addCorrupt(TransactionLockManager.LockType.WRITE).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                acquire();
      }
    };
    startFileHanlder.handleWithWriteLock(this);
  }

  /**
   * Create new or open an existing file for append.<p>
   *
   * In case of opening the file for append, the method returns the last block
   * of the file if this is a partial block, which can still be used for writing
   * more data. The client uses the returned block locations to form the data
   * pipeline for this block.<br> The method returns null if the last block is
   * full or if this is a new file. The client then allocates a new block with
   * the next call using {@link NameNode#addBlock()}.<p>
   *
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create()}
   *
   * @return the last block locations if the block is partial or null otherwise
   */
  private LocatedBlock startFileInternal(String src,
          PermissionStatus permissions, String holder, String clientMachine,
          EnumSet<CreateFlag> flag, boolean createParent, short replication,
          long blockSize) throws SafeModeException, FileAlreadyExistsException,
          AccessControlException, UnresolvedLinkException, FileNotFoundException,
          ParentNotDirectoryException, IOException, PersistanceException {
    assert isWritingNN();
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
              + ", holder=" + holder
              + ", clientMachine=" + clientMachine
              + ", createParent=" + createParent
              + ", replication=" + replication
              + ", createFlag=" + flag.toString());
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create file" + src, safeMode);
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }

    // Verify that the destination does not exist as a directory already.
    boolean pathExists = dir.exists(src);
    if (pathExists && dir.isDir(src)) {
      throw new FileAlreadyExistsException("Cannot create file " + src
              + "; already exists as a directory.");
    }

    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean append = flag.contains(CreateFlag.APPEND);
    if (isPermissionEnabled) {
      if (append || (overwrite && pathExists)) {
        checkPathAccess(src, FsAction.WRITE);
      } else {
        checkAncestorAccess(src, FsAction.WRITE);
      }
    }

    if (!createParent) {
      verifyParentDir(src);
    }

    try {
      INode myFile = dir.getFileINode(src);
      recoverLeaseInternal(myFile, src, holder, clientMachine, false);

      try {
        blockManager.verifyReplication(src, replication, clientMachine);
      } catch (IOException e) {
        throw new IOException("failed to create " + e.getMessage());
      }
      boolean create = flag.contains(CreateFlag.CREATE);
      if (myFile == null) {
        if (!create) {
          throw new FileNotFoundException("failed to overwrite or append to non-existent file "
                  + src + " on client " + clientMachine);
        }
      } else {
        // File exists - must be one of append or overwrite
        if (overwrite) {
          delete(src, true);
        } else if (!append) {
          throw new FileAlreadyExistsException("failed to create file " + src
                  + " on client " + clientMachine
                  + " because the file exists");
        }
      }

      final DatanodeDescriptor clientNode =
              blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);

      if (append && myFile != null) {
        //
        // Replace current node with a INodeUnderConstruction.
        // Recreate in-memory lease record.
        //
        INodeFile inodeFile = (INodeFile) myFile;
        inodeFile.convertToUnderConstruction(holder, clientMachine, clientNode);
        EntityManager.update(inodeFile);
        leaseManager.addLease(inodeFile.getClientName(), src);

        // convert last block to under-construction
        return blockManager.convertLastBlockToUnderConstruction(inodeFile);
      } else {
        // Now we can add the name to the filesystem. This file has no
        // blocks associated with it.
        //
        checkFsObjectLimit(OperationType.START_FILE); //FIXME the flow shouldnt reach here for TestBLockRecovery

        // increment global generation stamp
        long genstamp = nextGenerationStamp();
        INodeFile newNode = dir.addFile(src, permissions,
                replication, blockSize, holder, clientMachine, clientNode, genstamp);

        if (newNode == null) {
          throw new IOException("DIR* NameSystem.startFile: "
                  + "Unable to add file to namespace.");
        }
        assert newNode.isUnderConstruction();
        leaseManager.addLease(newNode.getClientName(), src);
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: "
                  + "add " + src + " to namespace for " + holder);
        }
      }
    } catch (IOException ie) {
      NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: "
              + ie.getMessage());
      throw ie;
    }
    return null;
  }

  /**
   * Recover lease; Immediately revoke the lease of the current lease holder and
   * start lease recovery so that the file can be forced to be closed.
   *
   * @param src the path of the file to start lease recovery
   * @param holder the lease holder's name
   * @param clientMachine the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(final String src, final String holder, final String clientMachine)
          throws IOException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }
    TransactionalRequestHandler recoverLeaseHandler = new TransactionalRequestHandler(OperationType.RECOVER_LEASE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isInSafeMode()) {
          throw new SafeModeException(
                  "Cannot recover the lease of " + src, safeMode);
        }
        if (!DFSUtil.isValidName(src)) {
          throw new IOException("Invalid file name: " + src);
        }

        INode inode = dir.getFileINode(src);
        if (inode == null) {
          throw new FileNotFoundException("File not found " + src);
        }

        if (!inode.isUnderConstruction()) {
          return true;
        }
        if (isPermissionEnabled) {
          checkPathAccess(src, FsAction.WRITE);
        }
        recoverLeaseInternal(inode, src, holder, clientMachine, true);
        return false;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{src},
                getFsDirectory().getRootDir()).
                addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE, holder).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                addUnderReplicatedBlock(LockType.WRITE).
                acquire();
      }
    };
    return (Boolean) recoverLeaseHandler.handleWithWriteLock(this);
  }

  private void recoverLeaseInternal(INode fileInode,
          String src, String holder, String clientMachine, boolean force)
          throws IOException, PersistanceException {
    assert isWritingNN();
    assert hasWriteLock();
    if (fileInode != null && fileInode.isUnderConstruction()) {
      INodeFile pendingFile = (INodeFile) fileInode;
      //
      // If the file is under construction , then it must be in our
      // leases. Find the appropriate lease record.
      //
      Lease lease = leaseManager.getLease(holder);
      //
      // We found the lease for this file. And surprisingly the original
      // holder is trying to recreate this file. This should never occur.
      //
      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLeaseByPath(src);
        if ((leaseFile != null && leaseFile.equals(lease))
                || lease.getHolder().equals(holder)) {
          throw new AlreadyBeingCreatedException(
                  "failed to create file " + src + " for " + holder
                  + " on client " + clientMachine
                  + " because current leaseholder is trying to recreate file.");
        }
      }
      //
      // Find the original holder.
      //
      lease = leaseManager.getLease(pendingFile.getClientName());
      if (lease == null) {
        throw new AlreadyBeingCreatedException(
                "failed to create file " + src + " for " + holder
                + " on client " + clientMachine
                + " because pendingCreates is non-null but no leases found.");
      }
      if (force) {
        // close now: no need to wait for soft lease expiration and 
        // close only the file src
        LOG.info("recoverLease: recover lease " + lease + ", src=" + src
                + " from client " + pendingFile.getClientName());
        internalReleaseLease(lease, src, holder);
      } else {
        assert lease.getHolder().equals(pendingFile.getClientName()) :
                "Current lease holder " + lease.getHolder()
                + " does not match file creator " + pendingFile.getClientName();
        //
        // If the original holder has not renewed in the last SOFTLIMIT 
        // period, then start lease recovery.
        //
        if (leaseManager.expiredSoftLimit(lease)) {
          LOG.info("startFile: recover lease " + lease + ", src=" + src
                  + " from client " + pendingFile.getClientName());
          boolean isClosed = internalReleaseLease(lease, src, null);
          if (!isClosed) {
            throw new RecoveryInProgressException(
                    "Failed to close file " + src
                    + ". Lease recovery is in progress. Try again later.");
          }
        } else {
          BlockInfo lastBlock = pendingFile.getLastBlock();
          if (lastBlock != null && lastBlock.getBlockUCState()
                  == BlockUCState.UNDER_RECOVERY) {
            throw new RecoveryInProgressException(
                    "Recovery in progress, file [" + src + "], "
                    + "lease owner [" + lease.getHolder() + "]");
          } else {
            throw new AlreadyBeingCreatedException(
                    "Failed to create file [" + src + "] for [" + holder
                    + "] on client [" + clientMachine
                    + "], because this file is already being created by ["
                    + pendingFile.getClientName() + "] on ["
                    + pendingFile.getClientMachine() + "]");
          }
        }
      }
    }

  }

  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(final String src, final String holder, final String clientMachine)
          throws AccessControlException, SafeModeException,
          FileAlreadyExistsException, FileNotFoundException,
          ParentNotDirectoryException, IOException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }
    if (supportAppends == false) {
      throw new UnsupportedOperationException("Append to hdfs not supported."
              + " Please refer to dfs.support.append configuration parameter.");
    }
    TransactionalRequestHandler appendFileHandler = new TransactionalRequestHandler(OperationType.APPEND_FILE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return startFileInternal(src, null, holder, clientMachine,
                EnumSet.of(CreateFlag.APPEND),
                false, blockManager.maxReplication, (long) 0);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{src}, getFsDirectory().getRootDir());
        tla.addBlock(TransactionLockManager.LockType.READ).
                addLease(TransactionLockManager.LockType.WRITE, holder).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                addUnderReplicatedBlock(TransactionLockManager.LockType.WRITE).
                addInvalidatedBlock(TransactionLockManager.LockType.WRITE).
                acquire();
      }
    };
    LocatedBlock lb = (LocatedBlock) appendFileHandler.handleWithWriteLock(this);
    //getEditLog().logSync();
    if (lb != null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: file "
                + src + " for " + holder + " at " + clientMachine
                + " block " + lb.getBlock()
                + " block size " + lb.getBlock().getNumBytes());
      }
    }
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
              Server.getRemoteIp(),
              "append", src, null, null);
    }
    return lb;
  }

  ExtendedBlock getExtendedBlock(Block blk) {
    return new ExtendedBlock(blockPoolId, blk);
  }

  void setBlockPoolId(String bpid) {
    //blockPoolId = bpid; //[thesis]
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to). Return an array that consists of the
   * block, plus a set of machines. The first on this list should be where the
   * client writes data. Subsequent items in the list must be provided in the
   * connection to the first datanode.
   *
   * Make sure the previous blocks have been reported by datanodes and are
   * replicated. Will return an empty 2-elt array if we want the client to "try
   * again later".
   */
  LocatedBlock getAdditionalBlockWithTransaction(final String src,
          final String clientName,
          final ExtendedBlock previous,
          final HashMap<Node, Node> excludedNodes)
          throws LeaseExpiredException, NotReplicatedYetException,
          QuotaExceededException, SafeModeException, UnresolvedLinkException,
          IOException {
    TransactionalRequestHandler additionalBlockHanlder = new TransactionalRequestHandler(OperationType.GET_ADDITIONAL_BLOCK) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return getAdditionalBlock(src, clientName, previous, excludedNodes);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{src}, getFsDirectory().getRootDir());
        tla.addBlock(TransactionLockManager.LockType.WRITE).
                addReplica(LockType.READ).
                addLease(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.WRITE).
                addExcess(TransactionLockManager.LockType.WRITE).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                acquire();
      }
    };
    return (LocatedBlock) additionalBlockHanlder.handleWithWriteLock(this);
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to). Return an array that consists of the
   * block, plus a set of machines. The first on this list should be where the
   * client writes data. Subsequent items in the list must be provided in the
   * connection to the first datanode.
   *
   * Make sure the previous blocks have been reported by datanodes and are
   * replicated. Will return an empty 2-elt array if we want the client to "try
   * again later".
   */
  LocatedBlock getAdditionalBlock(String src,
          String clientName,
          ExtendedBlock previous,
          HashMap<Node, Node> excludedNodes)
          throws LeaseExpiredException, NotReplicatedYetException,
          QuotaExceededException, SafeModeException, UnresolvedLinkException,
          IOException, PersistanceException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }

    checkBlock(previous);
    long fileLength, blockSize;
    int replication;
    DatanodeDescriptor clientNode = null;
    Block newBlock = null;
    final DatanodeDescriptor targets[];

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
              "BLOCK* NameSystem.getAdditionalBlock: file "
              + src + " for " + clientName);
    }

    try {

      if (isInSafeMode()) {
        throw new SafeModeException("Cannot add block to " + src, safeMode);
      }

      // have we exceeded the configured limit of fs objects.
      checkFsObjectLimit(OperationType.GET_ADDITIONAL_BLOCK);

      INodeFile pendingFile = checkLease(src, clientName);

      assert pendingFile.isUnderConstruction();
      /*
       * TODO[Hooman]: It seems that this operation is idempotent and can be
       * done in a separate transaction. It can be committed in the first
       * writelock and release the lock and try to take them again.
       */
      // commit the last block and complete it if it has minimum replicas
      commitOrCompleteLastBlock(pendingFile, ExtendedBlock.getLocalBlock(previous));

      //
      // If we fail this, bad things happen!
      //
      if (!checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }
      fileLength = pendingFile.computeContentSummary().getLength();
      blockSize = pendingFile.getPreferredBlockSize();
      clientNode = blockManager.getDatanodeManager().getDatanodeByName(pendingFile.getClientName());
      replication = (int) pendingFile.getReplication();
      /*
       * } finally { writeUnlock(); }
       */

      // choose targets for the new block to be allocated.
      targets = blockManager.chooseTarget(
              src, replication, clientNode, excludedNodes, blockSize);

      // Allocate a new block and record it in the INode. 
    /*
       * writeLock(); try {
       */
//      if (isInSafeMode()) {
//        throw new SafeModeException("Cannot add block to " + src, safeMode);
//      }
      INode[] pathINodes = dir.getExistingPathINodes(src);
      //[Hooman]: These checkings are not necessary when doing all in one writelock and one transaction.
      /*
       * int inodesLen = pathINodes.length; checkLease(src, clientName,
       * pathINodes[inodesLen-1]); INodeFileUnderConstruction pendingFile =
       * (INodeFileUnderConstruction) pathINodes[inodesLen - 1];
       */

      //[Hooman]: -1 selectUsingIndex
//      if (!checkFileProgress(pendingFile, false)) {
//        throw new NotReplicatedYetException("Not replicated yet:" + src);
//      }

      // allocate new block record block locations in INode.
      newBlock = allocateBlock(src, pathINodes, targets);

      for (DatanodeDescriptor dn : targets) {
        dn.incBlocksScheduled();
      }
    } finally {
    }

    // Create next block
    LocatedBlock b = new LocatedBlock(getExtendedBlock(newBlock), targets, fileLength);
    blockManager.setBlockToken(b, BlockTokenSecretManager.AccessMode.WRITE);
    return b;
  }

  /**
   * @see NameNode#getAdditionalDatanode(String, ExtendedBlock, DatanodeInfo[],
   * DatanodeInfo[], int, String)
   */
  LocatedBlock getAdditionalDatanode(final String src, final ExtendedBlock blk,
          final DatanodeInfo[] existings, final HashMap<Node, Node> excludes,
          final int numAdditionalNodes, final String clientName) throws IOException {
    TransactionalRequestHandler getAdditionalDatanodeHandler = new TransactionalRequestHandler(OperationType.GET_ADDITIONAL_DATANODE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {

        //check if the feature is enabled
        dtpReplaceDatanodeOnFailure.checkEnabled();

        final DatanodeDescriptor clientnode;
        final long preferredblocksize;
        final List<DatanodeDescriptor> chosen;
        //check safe mode
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot add datanode; src=" + src
                  + ", blk=" + blk, safeMode);
        }

        //check lease
        final INodeFile file = checkLease(src, clientName);
        assert file.isUnderConstruction();
        clientnode = blockManager.getDatanodeManager().getDatanode(file.getClientNode());
        preferredblocksize = file.getPreferredBlockSize();

        //find datanode descriptors
        chosen = new ArrayList<DatanodeDescriptor>();
        for (DatanodeInfo d : existings) {
          final DatanodeDescriptor descriptor = blockManager.getDatanodeManager().getDatanode(d);
          if (descriptor != null) {
            chosen.add(descriptor);
          }
        }

        // choose new datanodes.
        final DatanodeInfo[] targets = blockManager.getBlockPlacementPolicy().chooseTarget(src, numAdditionalNodes, clientnode, chosen, true,
                excludes, preferredblocksize);
        final LocatedBlock lb = new LocatedBlock(blk, targets);
        blockManager.setBlockToken(lb, AccessMode.COPY);
        return lb;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.READ,
                new String[]{src}, getFsDirectory().getRootDir()).
                addLease(TransactionLockManager.LockType.READ).
                acquire();
      }
    };
    return (LocatedBlock) getAdditionalDatanodeHandler.handleWithReadLock(this);
  }

  /**
   * The client would like to let go of the given block
   */
  boolean abandonBlock(final ExtendedBlock b, final String src, final String holder)
          throws LeaseExpiredException, FileNotFoundException,
          UnresolvedLinkException, IOException {
    TransactionalRequestHandler abandonBlockHandler = new TransactionalRequestHandler(OperationType.ABANDON_BLOCK) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        //
        // Remove the block from the pending creates list
        //
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                  + b + "of file " + src);
        }
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot abandon block " + b
                  + " for fle" + src, safeMode);
        }

        INodeFile file = checkLease(src, holder);
        assert file.isUnderConstruction();
        dir.removeBlock(src, file, ExtendedBlock.getLocalBlock(b));

        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                  + b + " is removed from pendingCreates");
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE_ON_PARENT,
                new String[]{src}, getFsDirectory().getRootDir()).
                addReplica(LockType.WRITE).
                addBlock(LockType.WRITE).
                addLease(TransactionLockManager.LockType.READ).
                addCorrupt(LockType.WRITE).
                addReplicaUc(LockType.WRITE).
                acquire();
      }
    };
    abandonBlockHandler.handleWithWriteLock(this);
    return true;
  }

  // make sure that we still have the lease on this file.
  private INodeFile checkLease(String src, String holder)
          throws LeaseExpiredException, UnresolvedLinkException, PersistanceException {
    assert hasReadOrWriteLock();
    INodeFile file = dir.getFileINode(src);
    checkLease(src, holder, file);
    return file;
  }

  private void checkLease(String src, String holder, INode file)
          throws LeaseExpiredException, PersistanceException {
    assert isWritingNN();
    assert hasReadOrWriteLock();
    if (file == null || file.isDirectory()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src
              + " File does not exist. "
              + (lease != null ? lease.toString()
              : "Holder " + holder
              + " does not have any open files."));
    }

    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src
              + " File is not open for writing. "
              + (lease != null ? lease.toString()
              : "Holder " + holder
              + " does not have any open files."));
    }
    INodeFile pendingFile = (INodeFile) file;
    if (holder != null && !pendingFile.getClientName().equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " + src + " owned by "
              + pendingFile.getClientName() + " but is accessed by " + holder);
    }
  }

  /**
   * Complete in-progress write to the given file.
   *
   * @return true if successful, false if the client should continue to retry
   * (e.g if not all blocks have reached minimum replication yet)
   * @throws IOException on error (eg lease mismatch, file not open, file
   * deleted)
   */
  boolean completeFileOld(String src, String holder, ExtendedBlock last)
          throws SafeModeException, UnresolvedLinkException, IOException, PersistanceException {
    checkBlock(last);
    boolean success = false;
    writeLock();
    try {
      success = completeFileInternal(src, holder,
              ExtendedBlock.getLocalBlock(last));
    } finally {
      writeUnlock();
    }
    return success;
  }

  /**
   * Complete in-progress write to the given file.
   *
   * @return true if successful, false if the client should continue to retry
   * (e.g if not all blocks have reached minimum replication yet)
   * @throws IOException on error (eg lease mismatch, file not open, file
   * deleted)
   */
  boolean completeFile(final String src, final String holder, final ExtendedBlock last)
          throws SafeModeException, UnresolvedLinkException, IOException {
    assert isWritingNN();
    checkBlock(last);
    TransactionalRequestHandler completeFileHandler = new TransactionalRequestHandler(OperationType.COMPLETE_FILE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        boolean success = completeFileInternal(src, holder,
                ExtendedBlock.getLocalBlock(last));
        if (success) {
          NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src
                  + " is closed by " + holder);
        }

        return success;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{src}, getFsDirectory().getRootDir());
        tla.addBlock(TransactionLockManager.LockType.WRITE).
                addLease(TransactionLockManager.LockType.WRITE).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                addUnderReplicatedBlock(LockType.WRITE).
                acquire();
      }
    };
    return (Boolean) completeFileHandler.handleWithWriteLock(this);
  }

  private boolean completeFileInternal(String src,
          String holder, Block last) throws SafeModeException,
          UnresolvedLinkException, IOException, PersistanceException {
    assert hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: "
              + src + " for " + holder);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot complete file " + src, safeMode);
    }

    INodeFile pendingFile = checkLease(src, holder);
    assert pendingFile.isUnderConstruction();
    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, last);

    if (!checkFileProgress(pendingFile, true)) {
      return false;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile);

    return true;
  }

  /**
   * Check all blocks of a file. If any blocks are lower than their intended
   * replication factor, then insert them into neededReplication
   *
   * @throws IOException
   */
  private void checkReplicationFactor(INodeFile file) throws IOException, PersistanceException {

    int numExpectedReplicas = file.getReplication();
    List<BlockInfo> pendingBlocks = file.getBlocks();
    for (BlockInfo blockInfo : pendingBlocks) {
      blockManager.checkReplication(blockInfo, numExpectedReplicas);
    }
  }

  /**
   * Allocate a block at the given pending filename
   *
   * @param src path to the file
   * @param inodes INode representing each of the components of src.
   * <code>inodes[inodes.length-1]</code> is the INode for the file.
   * @throws IOException
   */
  private Block allocateBlock(String src, INode[] inodes,
          DatanodeDescriptor targets[]) throws IOException, PersistanceException {
    assert hasWriteLock();
    Block b = new Block(DFSUtil.getRandom().nextLong(), 0, 0);
    // FIXME not allowed to check a new bid in the db
//    while (isValidBlock(b)) {
//      b.setBlockId(DFSUtil.getRandom().nextLong());
//    }
    b.setGenerationStamp(getGenerationStamp());
    b = dir.addBlock(src, inodes, b, targets);
    NameNode.stateChangeLog.info("BLOCK* NameSystem.allocateBlock: "
            + src + ". " + blockPoolId + " " + b);
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and replicated. If not,
   * return false. If checkall is true, then check all blocks, otherwise check
   * only penultimate block.
   *
   * @throws IOException
   */
  boolean checkFileProgress(INodeFile v, boolean checkall) throws IOException, PersistanceException {
    readLock();
    try {
      if (checkall) {
        //
        // check all blocks of the file.
        //
        for (BlockInfo block : v.getBlocks()) {
          if (!block.isComplete()) {
            LOG.info("BLOCK* NameSystem.checkFileProgress: "
                    + "block " + block + " has not reached minimal replication "
                    + blockManager.minReplication);
            return false;
          }
        }
      } else {
        //
        // check the penultimate block of this file
        //
        BlockInfo b = v.getPenultimateBlock();
        if (b != null && !b.isComplete()) {
          LOG.info("BLOCK* NameSystem.checkFileProgress: "
                  + "block " + b + " has not reached minimal replication "
                  + blockManager.minReplication);
          return false;
        }
      }
      return true;
    } finally {
      readUnlock();
    }
  }

  ////////////////////////////////////////////////////////////////
  // Here's how to handle block-copy failure during client write:
  // -- As usual, the client's write should result in a streaming
  // backup write to a k-machine sequence.
  // -- If one of the backup machines fails, no worries.  Fail silently.
  // -- Before client is allowed to close and finalize file, make sure
  // that the blocks are backed up.  Namenode may have to issue specific backup
  // commands to make up for earlier datanode failures.  Once all copies
  // are made, edit namespace and return to client.
  ////////////////////////////////////////////////////////////////
  /**
   * Change the indicated filename.
   *
   * @deprecated Use {@link #renameTo(String, String, Options.Rename...)}
   * instead.
   */
  @Deprecated
  boolean renameTo(final String src, final String dst)
          throws IOException, UnresolvedLinkException, ImproperUsageException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }
    TransactionalRequestHandler renameToHandler = new TransactionalRequestHandler(OperationType.RENAME_TO) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        boolean status = false;
        HdfsFileStatus resultingStat = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src
                  + " to " + dst);
        }
        status = renameToInternal(src, dst);
        if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
          resultingStat = dir.getFileInfo(dst, false);
        }
        //getEditLog().logSync();
        if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                  Server.getRemoteIp(),
                  "rename", src, dst, resultingStat);
        }
        return status;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
    return (Boolean) renameToHandler.handleWithWriteLock(this);
  }

  /**
   * @deprecated See {@link #renameTo(String, String)}
   */
  @Deprecated
  private boolean renameToInternal(String src, String dst)
          throws IOException, UnresolvedLinkException, PersistanceException {
    assert isWritingNN();
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      //We should not be doing this.  This is move() not renameTo().
      //but for now,
      String actualdst = dir.isDir(dst)
              ? dst + Path.SEPARATOR + new Path(src).getName() : dst;
      checkParentAccess(src, FsAction.WRITE);
      checkAncestorAccess(actualdst, FsAction.WRITE);
    }

    HdfsFileStatus dinfo = dir.getFileInfo(dst, false);

    boolean isRenameDone = false;
    if (dir.renameTo(src, dst)) {
      unprotectedChangeLease(src, dst, dinfo);     // update lease with new filename
      isRenameDone = true;
    } else {
      isRenameDone = false;
    }

    return isRenameDone;
  }

  /**
   * Rename src to dst
   */
  void renameTo(final String src, final String dst, final Options.Rename... options)
          throws IOException, UnresolvedLinkException {
    TransactionalRequestHandler renameTo2Hanlder = new TransactionalRequestHandler(OperationType.RENAME_TO2) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        HdfsFileStatus resultingStat = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options - "
                  + src + " to " + dst);
        }
        renameToInternal(src, dst, options);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          resultingStat = dir.getFileInfo(dst, false);
        }

        //getEditLog().logSync();
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          StringBuilder cmd = new StringBuilder("rename options=");
          for (Rename option : options) {
            cmd.append(option.value()).append(" ");
          }
          logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(),
                  cmd.toString(), src, dst, resultingStat);
        }
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
    renameTo2Hanlder.handleWithWriteLock(this);
  }

  private void renameToInternal(String src, String dst, Options.Rename... options) throws IOException, PersistanceException {
    assert isWritingNN();
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      checkParentAccess(src, FsAction.WRITE);
      checkAncestorAccess(dst, FsAction.WRITE);
    }

    HdfsFileStatus dinfo = dir.getFileInfo(dst, false);

    dir.renameTo(src, dst, options);
    unprotectedChangeLease(src, dst, dinfo); // update lease with new filename
  }

  /**
   * Remove the indicated file from namespace.
   *
   * @see ClientProtocol#delete(String, boolean) for detailed descriptoin and
   * description of exceptions
   */
  boolean deleteWithTransaction(final String src, final boolean recursive)
          throws AccessControlException, SafeModeException,
          UnresolvedLinkException, IOException {
    TransactionalRequestHandler deleteHandler = new TransactionalRequestHandler(OperationType.DELETE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return delete(src, recursive);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{src},
                getFsDirectory().getRootDir()).
                addLease(TransactionLockManager.LockType.WRITE).
                addLeasePath(TransactionLockManager.LockType.WRITE).
                addBlock(TransactionLockManager.LockType.WRITE).
                addReplica(TransactionLockManager.LockType.WRITE).
                addCorrupt(TransactionLockManager.LockType.WRITE).
                addReplicaUc(TransactionLockManager.LockType.WRITE).
                acquire();
      }
    };
    return (Boolean) deleteHandler.handleWithWriteLock(this);
  }

  /**
   * Remove the indicated file from namespace.
   *
   * @see ClientProtocol#delete(String, boolean) for detailed descriptoin and
   * description of exceptions
   */
  boolean delete(String src, boolean recursive)
          throws AccessControlException, SafeModeException,
          UnresolvedLinkException, IOException, PersistanceException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }
    boolean status = deleteInternal(src, recursive, true);
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
              Server.getRemoteIp(),
              "delete", src, null, null);
    }
    return status;
  }

  /**
   * Remove a file/directory from the namespace. <p> For large directories,
   * deletion is incremental. The blocks under the directory are collected and
   * deleted a small number at a time holding the {@link FSNamesystem} lock. <p>
   * For small directory or file the deletion is done in one shot.
   *
   * @see ClientProtocol#delete(String, boolean) for description of exceptions
   */
  private boolean deleteInternal(String src, boolean recursive,
          boolean enforcePermission)
          throws AccessControlException, SafeModeException, UnresolvedLinkException,
          IOException, PersistanceException {
    ArrayList<Block> collectedBlocks = new ArrayList<Block>();

    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot delete " + src, safeMode);
      }
      if (!recursive && !dir.isDirEmpty(src)) {
        throw new IOException(src + " is non empty");
      }
      if (enforcePermission && isPermissionEnabled) {
        checkPermission(src, false, null, FsAction.WRITE, null, FsAction.ALL);
      }
      // Unlink the target directory from directory tree

      if (!dir.delete(src, collectedBlocks)) {
        return false;
      }
      removeBlocks(collectedBlocks);
    } finally {
    }
    collectedBlocks.clear();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "
              + src + " is removed");
    }
    return true;
  }

  /**
   * From the given list, incrementally remove the blocks from blockManager
   *
   * @throws IOException
   */
  private void removeBlocks(List<Block> blocks) throws IOException, PersistanceException {
    assert hasWriteLock();
    int start = 0;
    int end = 0;
    LOG.debug("Inside removeBlocks with size " + blocks.size());
    while (start < blocks.size()) {
      end = BLOCK_DELETION_INCREMENT + start;
      end = end > blocks.size() ? blocks.size() : end;
      for (int i = start; i < end; i++) {
        blockManager.removeBlock(blocks.get(i));
      }
      start = end;
    }
  }

  void removePathAndBlocks(String src, List<Block> blocks) throws IOException, PersistanceException {
    assert isWritingNN();
    assert hasWriteLock();
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    for (Block b : blocks) {
      blockManager.removeBlock(b);
    }
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src The string representation of the path to the file
   * @param resolveLink whether to throw UnresolvedLinkException if src refers
   * to a symlinks
   *
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if a symlink is encountered.
   *
   * @return object containing information regarding the file or null if file
   * not found
   */
  HdfsFileStatus getFileInfo(final String src, final boolean resolveLink)
          throws AccessControlException, UnresolvedLinkException, IOException {
    TransactionalRequestHandler getFileInfoHandler = new TransactionalRequestHandler(OperationType.GET_FILE_INFO) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (!DFSUtil.isValidName(src)) {
          throw new InvalidPathException("Invalid file name: " + src);
        }
        if (isPermissionEnabled) {
          checkTraverse(src);
        }

        return dir.getFileInfo(src, resolveLink);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.READ,
                new String[]{src}, getFsDirectory().getRootDir());
        tla.addBlock(TransactionLockManager.LockType.READ).
                acquire();
      }
    };
    return (HdfsFileStatus) getFileInfoHandler.handleWithReadLock(this);
  }
  
  /**
   * Create all the necessary directories
   */
  boolean mkdirs(final String src, final PermissionStatus permissions,
          final boolean createParent) throws IOException, UnresolvedLinkException {
    TransactionalRequestHandler mkdirsHanlder = new TransactionalRequestHandler(OperationType.MKDIRS) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        boolean status = false;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
        }
        status = mkdirsInternal(src, permissions, createParent);
        //getEditLog().logSync();
        if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
          final HdfsFileStatus stat = dir.getFileInfo(src, false);
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                  Server.getRemoteIp(),
                  "mkdirs", src, null, stat);
        }
        return status;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH_WITH_UNKNOWN_HEAD,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{src},
                getFsDirectory().getRootDir()).
                acquire();
      }
    };
    return (Boolean) mkdirsHanlder.handleWithWriteLock(this);
  }

  /**
   * Create all the necessary directories
   */
  private boolean mkdirsInternal(String src,
          PermissionStatus permissions, boolean createParent)
          throws IOException, UnresolvedLinkException, PersistanceException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create directory " + src, safeMode);
    }
    if (isPermissionEnabled) {
      checkTraverse(src);
    }
    if (dir.isDir(src)) {
      // all the users of mkdirs() are used to expect 'true' even if
      // a new directory is not created.
      return true;
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    if (isPermissionEnabled) {
      checkAncestorAccess(src, FsAction.WRITE);
    }
    if (!createParent) {
      verifyParentDir(src);
    }

    // validate that we have enough inodes. This is, at best, a 
    // heuristic because the mkdirs() operation migth need to 
    // create multiple inodes.
    checkFsObjectLimit(OperationType.MKDIRS);

    if (!dir.mkdirs(src, permissions, false, now())) {
      throw new IOException("Failed to create directory: " + src);
    }
    return true;
  }

  ContentSummary getContentSummary(final String src) throws AccessControlException,
          FileNotFoundException, UnresolvedLinkException, IOException {
    TransactionalRequestHandler getContentSummaryHandler = new TransactionalRequestHandler(OperationType.GET_CONTENT_SUMMARY) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isPermissionEnabled) {
          checkPermission(src, false, null, null, null, FsAction.READ_EXECUTE);
        }
        return dir.getContentSummary(src);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY,
                TransactionLockManager.INodeLockType.READ,
                new String[]{src}, getFsDirectory().getRootDir());
        tla.addBlock(TransactionLockManager.LockType.READ).
                acquire();
      }
    };
    return (ContentSummary) getContentSummaryHandler.handleWithReadLock(this);
  }

  /**
   * Set the namespace quota and diskspace quota for a directory. See {@link ClientProtocol#setQuota(String, long, long)}
   * for the contract.
   */
  void setQuota(final String path, final long nsQuota, final long dsQuota)
          throws IOException, UnresolvedLinkException {
    TransactionalRequestHandler setQuotaHanlder = new TransactionalRequestHandler(OperationType.SET_QUOTA) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot set quota on " + path, safeMode);
        }
        if (isPermissionEnabled) {
          checkSuperuserPrivilege();
        }
        dir.setQuota(path, nsQuota, dsQuota);
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.WRITE,
                new String[]{path}, getFsDirectory().getRootDir()).
                acquire();
      }
    };
    setQuotaHanlder.handleWithWriteLock(this);
  }

  /**
   * Persist all metadata about this file.
   *
   * @param src The string representation of the path
   * @param clientName The string representation of the client
   * @throws IOException if path does not exist
   */
  void fsync(final String src, final String clientName)
          throws IOException, UnresolvedLinkException {
    NameNode.stateChangeLog.info("BLOCK* NameSystem.fsync: file "
            + src + " for " + clientName);
    TransactionalRequestHandler fsyncHandler = new TransactionalRequestHandler(OperationType.FSYNC) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot fsync file " + src, safeMode);
        }
        INodeFile pendingFile = checkLease(src, clientName);
        assert pendingFile.isUnderConstruction();
        dir.persistBlocks(src, pendingFile);
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.ONLY_PATH,
                TransactionLockManager.INodeLockType.READ,
                new String[]{src}, getFsDirectory().getRootDir()).
                addBlock(LockType.READ).
                addLease(TransactionLockManager.LockType.READ).
                acquire();
      }
    };
    fsyncHandler.handleWithWriteLock(this);
    //getEditLog().logSync();
  }

  /**
   * Move a file that is being written to be immutable.
   *
   * @param src The filename
   * @param lease The lease for the client creating the file
   * @param recoveryLeaseHolder reassign lease to this holder if the last block
   * needs recovery; keep current holder if null.
   * @throws AlreadyBeingCreatedException if file is waiting to achieve minimal
   * replication;<br> RecoveryInProgressException if lease recovery is in
   * progress.<br> IOException in case of an error.
   * @return true if file has been successfully finalized and closed or false if
   * block recovery has been initiated
   */
  boolean internalReleaseLease(Lease lease, String src,
          String recoveryLeaseHolder) throws AlreadyBeingCreatedException,
          IOException, UnresolvedLinkException, ImproperUsageException, PersistanceException {
    LOG.info("Recovering lease=" + lease + ", src=" + src);
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }

    assert !isInSafeMode();
    assert hasWriteLock();
    INodeFile iFile = dir.getFileINode(src);
    if (iFile == null) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
              + "attempt to release a create lock on "
              + src + " file does not exist.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    if (!iFile.isUnderConstruction()) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
              + "attempt to release a create lock on "
              + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    INodeFile pendingFile = (INodeFile) iFile;
    assert pendingFile.isUnderConstruction();
    int nrBlocks = pendingFile.getBlocks().size();

    int nrCompleteBlocks;
    BlockInfo curBlock = null;
    for (nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks; nrCompleteBlocks++) {
      curBlock = pendingFile.getBlocks().get(nrCompleteBlocks);
      if (!curBlock.isComplete()) {
        break;
      }
      assert blockManager.checkMinReplication(curBlock) :
              "A COMPLETE block is not minimally replicated in " + src;
    }

    // If there are no incomplete blocks associated with this file,
    // then reap lease immediately and close the file.
    if (nrCompleteBlocks == nrBlocks) {
      finalizeINodeFileUnderConstruction(src, pendingFile);
      NameNode.stateChangeLog.warn("BLOCK*"
              + " internalReleaseLease: All existing blocks are COMPLETE,"
              + " lease removed, file closed.");
      return true;  // closed!
    }

    // Only the last and the penultimate blocks may be in non COMPLETE state.
    // If the penultimate block is not COMPLETE, then it must be COMMITTED.
    if (nrCompleteBlocks < nrBlocks - 2
            || nrCompleteBlocks == nrBlocks - 2
            && curBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      final String message = "DIR* NameSystem.internalReleaseLease: "
              + "attempt to release a create lock on "
              + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    // no we know that the last block is not COMPLETE, and
    // that the penultimate block if exists is either COMPLETE or COMMITTED
    BlockInfoUnderConstruction lastBlock = (BlockInfoUnderConstruction) pendingFile.getLastBlock();
    BlockUCState lastBlockState = lastBlock.getBlockUCState();
    BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();
    boolean penultimateBlockMinReplication;
    BlockUCState penultimateBlockState;
    if (penultimateBlock == null) {
      penultimateBlockState = BlockUCState.COMPLETE;
      // If penultimate block doesn't exist then its minReplication is met
      penultimateBlockMinReplication = true;
    } else {
      penultimateBlockState = BlockUCState.COMMITTED;
      penultimateBlockMinReplication =
              blockManager.checkMinReplication(penultimateBlock);
    }
    assert penultimateBlockState == BlockUCState.COMPLETE
            || penultimateBlockState == BlockUCState.COMMITTED :
            "Unexpected state of penultimate block in " + src;

    switch (lastBlockState) {
      case COMPLETE:
        assert false : "Already checked that the last block is incomplete";
        break;
      case COMMITTED:
        //XXX [H]: It seems the following if statement never becomes true
        // Close file if committed blocks are minimally replicated
        if (penultimateBlockMinReplication
                && blockManager.checkMinReplication(lastBlock)) {
          finalizeINodeFileUnderConstruction(src, pendingFile);
          NameNode.stateChangeLog.warn("BLOCK*"
                  + " internalReleaseLease: Committed blocks are minimally replicated,"
                  + " lease removed, file closed.");
          return true;  // closed!
        }
        // Cannot close file right now, since some blocks 
        // are not yet minimally replicated.
        // This may potentially cause infinite loop in lease recovery
        // if there are no valid replicas on data-nodes.
        String message = "DIR* NameSystem.internalReleaseLease: "
                + "Failed to release lease for file " + src
                + ". Committed blocks are waiting to be minimally replicated."
                + " Try again later.";
        NameNode.stateChangeLog.warn(message);
        throw new AlreadyBeingCreatedException(message);
      case UNDER_CONSTRUCTION:
      case UNDER_RECOVERY:
        // start recovery of the last block for this file
        long blockRecoveryId = nextGenerationStamp();
        lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);
        lastBlock.initializeBlockRecovery(blockRecoveryId, getBlockManager().getDatanodeManager());
        EntityManager.update(lastBlock);
        leaseManager.renewLease(lease);
        // Cannot close file right now, since the last block requires recovery.
        // This may potentially cause infinite loop in lease recovery
        // if there are no valid replicas on data-nodes.
        NameNode.stateChangeLog.warn(
                "DIR* NameSystem.internalReleaseLease: "
                + "File " + src + " has not been closed."
                + " Lease recovery is in progress. "
                + "RecoveryId = " + blockRecoveryId + " for block " + lastBlock);
        break;
    }
    return false;
  }

  private Lease reassignLease(Lease lease, String src, String newHolder,
          INodeFile pendingFile) throws IOException, PersistanceException {
    assert pendingFile.isUnderConstruction();
    assert isWritingNN();
    assert hasWriteLock();
    if (newHolder == null) {
      return lease;
    }
    logReassignLease(lease.getHolder(), src, newHolder); //FIXME remove
    return reassignLeaseInternal(lease, src, newHolder, pendingFile);
  }

  Lease reassignLeaseInternal(Lease lease, String src, String newHolder,
          INodeFile pendingFile) throws IOException, PersistanceException {
    assert pendingFile.isUnderConstruction();
    assert isWritingNN();
    assert hasWriteLock();
    pendingFile.setClientName(newHolder);
    EntityManager.update(pendingFile);
    return leaseManager.reassignLease(lease, src, newHolder);
  }

  private void commitOrCompleteLastBlock(final INodeFile fileINode,
          final Block commitBlock) throws IOException, PersistanceException {
    assert fileINode.isUnderConstruction();
    assert isWritingNN();
    assert hasWriteLock();
    if (!blockManager.commitOrCompleteLastBlock(fileINode, commitBlock)) {
      return;
    }

    // [lock] the following acquires write-lock through the root.
    if (getFsDirectory().isQuotaEnabled()) {
      // Adjust disk space consumption if required
      final long diff = fileINode.getPreferredBlockSize() - commitBlock.getNumBytes();
      if (diff > 0) {
        try {
          String path = leaseManager.findPath(fileINode);
          dir.updateSpaceConsumed(path, 0, -diff * fileINode.getReplication());
        } catch (IOException e) {
          LOG.warn("Unexpected exception while updating disk space.", e);
        }
      }
    }
  }

  private void finalizeINodeFileUnderConstruction(String src,
          INodeFile pendingFile)
          throws IOException, UnresolvedLinkException, PersistanceException {
    assert pendingFile.isUnderConstruction();
    assert isWritingNN();
    assert hasWriteLock();
    leaseManager.removeLease(pendingFile.getClientName(), src);

    // The file is no longer pending.
    // Create permanent INode, update blocks
    pendingFile.convertToCompleteInode();
    EntityManager.update(pendingFile);
    //modification time which is modified in replaceNode().
    // close file and persist block allocations for this file
    dir.closeFile(src, pendingFile);

    checkReplicationFactor(pendingFile);
  }

  void commitBlockSynchronization(final ExtendedBlock lastblock,
          final long newgenerationstamp, final long newlength,
          final boolean closeFile, final boolean deleteblock, final DatanodeID[] newtargets)
          throws IOException, UnresolvedLinkException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }

    TransactionalRequestHandler commitBlockSyncHanlder = new TransactionalRequestHandler(OperationType.COMMIT_BLOCK_SYNCHRONIZATION) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        String src = "";

        if (isInSafeMode()) {
          throw new SafeModeException(
                  "Cannot commitBlockSynchronization while in safe mode",
                  safeMode);
        }
        LOG.info("commitBlockSynchronization(lastblock=" + lastblock
                + ", newgenerationstamp=" + newgenerationstamp
                + ", newlength=" + newlength
                + ", newtargets=" + Arrays.asList(newtargets)
                + ", closeFile=" + closeFile
                + ", deleteBlock=" + deleteblock
                + ")");
        final BlockInfo storedBlock = blockManager.getStoredBlock(ExtendedBlock.getLocalBlock(lastblock));
        if (storedBlock == null) {
          throw new IOException("Block (=" + lastblock + ") not found");
        }
        INodeFile iFile = storedBlock.getINode();
        if (!iFile.isUnderConstruction() || storedBlock.isComplete()) {
          throw new IOException("Unexpected block (=" + lastblock
                  + ") since the file (=" + iFile.getName()
                  + ") is not under construction");
        }

        long recoveryId =
                ((BlockInfoUnderConstruction) storedBlock).getBlockRecoveryId();
        if (recoveryId != newgenerationstamp) { //FIXME: [thesis] this exception fill be fixed once recoveryID is stored in DB
          throw new IOException("The recovery id " + newgenerationstamp
                  + " does not match current recovery id "
                  + recoveryId + " for block " + lastblock);
        }

        assert iFile.isUnderConstruction();

        if (deleteblock) {
          blockManager.removeBlockFromMap(storedBlock);
        } else {
          // update last block
          storedBlock.setGenerationStamp(newgenerationstamp);
          storedBlock.setNumBytes(newlength);
          EntityManager.update(storedBlock);

          // find the DatanodeDescriptor objects
          // There should be no locations in the blockManager till now because the
          // file is underConstruction
          DatanodeDescriptor[] descriptors = null;
          if (newtargets.length > 0) {
            descriptors = new DatanodeDescriptor[newtargets.length];
            for (int i = 0; i < newtargets.length; i++) {
              descriptors[i] = blockManager.getDatanodeManager().getDatanode(
                      newtargets[i]);
            }
          }
          if (closeFile) {
            // the file is getting closed. Insert block locations into blockManager.
            // Otherwise fsck will report these blocks as MISSING, especially if the
            // blocksReceived from Datanodes take a long time to arrive.
            for (DatanodeID id : newtargets) {
              IndexedReplica replica = storedBlock.addReplica(blockManager.getDatanodeManager().getDatanodeByStorageId(id.getStorageID()));
              if (replica != null) {
                EntityManager.add(replica);
              }
            }
          }
          // add pipeline locations into the INodeUnderConstruction
          BlockInfoUnderConstruction bUc = iFile.setLastBlock(storedBlock);
          for (DatanodeID id : newtargets) {
            ReplicaUnderConstruction addedReplica = bUc.addExpectedReplica(id.getStorageID(), HdfsServerConstants.ReplicaState.RBW);

            if (addedReplica != null) {
              EntityManager.add(addedReplica);
            }
          }

          EntityManager.update(storedBlock);
        }

        src = leaseManager.findPath(iFile);
        if (closeFile) {
          // commit the last block and complete it if it has minimum replicas
          commitOrCompleteLastBlock(iFile, storedBlock);

          //remove lease, close file
          finalizeINodeFileUnderConstruction(src, iFile);
        } else if (supportAppends) {
          // If this commit does not want to close the file, persist
          // blocks only if append is supported 
          dir.persistBlocks(src, iFile);
        }

        return src;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(INodeLockType.WRITE).
                addBlock(LockType.WRITE, lastblock.getBlockId()).
                addLease(LockType.WRITE).
                addLeasePath(LockType.WRITE).
                addReplica(LockType.WRITE).
                addCorrupt(LockType.WRITE).
                addExcess(LockType.READ).
                addReplicaUc(LockType.WRITE).
                addUnderReplicatedBlock(LockType.WRITE).
                acquireByBlock();
      }
    };
    String src = (String) commitBlockSyncHanlder.handleWithWriteLock(this);

    //getEditLog().logSync();
    if (closeFile) {
      LOG.info("commitBlockSynchronization(newblock=" + lastblock
              + ", file=" + src
              + ", newgenerationstamp=" + newgenerationstamp
              + ", newlength=" + newlength
              + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
    } else {
      LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
    }
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(final String holder) throws ImproperUsageException, IOException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }
    TransactionalRequestHandler renewLeaseHandler = new TransactionalRequestHandler(OperationType.RENEW_LEASE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot renew lease for " + holder, safeMode);
        }
        leaseManager.renewLease(holder);
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addLease(TransactionLockManager.LockType.WRITE, holder).
                acquire();
      }
    };
    renewLeaseHandler.handleWithWriteLock(this);
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src the directory name
   * @param startAfter the name to start after
   * @param needLocation if blockLocations need to be returned
   * @return a partial listing starting after startAfter
   *
   * @throws AccessControlException if access is denied
   * @throws UnresolvedLinkException if symbolic link is encountered
   * @throws IOException if other I/O error occurred
   */
  DirectoryListing getListing(final String src, final byte[] startAfter,
          final boolean needLocation)
          throws AccessControlException, UnresolvedLinkException, IOException {
    TransactionalRequestHandler getListingHandler = new TransactionalRequestHandler(OperationType.GET_LISTING) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isPermissionEnabled) {
          if (dir.isDir(src)) {
            checkPathAccess(src, FsAction.READ_EXECUTE);
          } else {
            checkTraverse(src);
          }
        }

        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          logAuditEvent(UserGroupInformation.getCurrentUser(),
                  Server.getRemoteIp(),
                  "listStatus", src, null, null);
        }
        return dir.getListing(src, startAfter, needLocation);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager tla = new TransactionLockManager();
        tla.addINode(TransactionLockManager.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN,
                TransactionLockManager.INodeLockType.READ,
                new String[]{src},
                getFsDirectory().getRootDir()).
                addBlock(TransactionLockManager.LockType.READ).
                addReplica(TransactionLockManager.LockType.READ).
                addExcess(TransactionLockManager.LockType.READ).
                addCorrupt(TransactionLockManager.LockType.READ).
                addReplicaUc(TransactionLockManager.LockType.READ).
                acquire();
      }
    };
    return (DirectoryListing) getListingHandler.handleWithReadLock(this);
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by datanodes
  //
  /////////////////////////////////////////////////////////
  /**
   * Register Datanode. <p> The purpose of registration is to identify whether
   * the new datanode serves a new data storage, and will report new data block
   * copies, which the namenode was not aware of; or the datanode is a
   * replacement node for the data storage that was previously served by a
   * different or the same (in terms of host:port) datanode. The data storages
   * are distinguished by their storageIDs. When a new data storage is reported
   * the namenode issues a new unique storageID. <p> Finally, the namenode
   * returns its namespaceID as the registrationID for the datanodes.
   * namespaceID is a persistent attribute of the name space. The registrationID
   * is checked every time the datanode is communicating with the namenode.
   * Datanodes with inappropriate registrationID are rejected. If the namenode
   * stops, and then restarts it can restore its namespaceID and will continue
   * serving the datanodes that has previously registered with the namenode
   * without restarting the whole cluster.
   *
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode
   */
  void registerDatanode(final DatanodeRegistration nodeReg) throws IOException {
    writeLock();
    try {
      getBlockManager().getDatanodeManager().registerDatanode(nodeReg, true, OperationType.REGISTER_DATANODE);
      new TransactionalRequestHandler(OperationType.REGISTER_DATANODE) {

        @Override
        public Object performTask() throws PersistanceException, IOException {
          checkSafeMode();
          return null;
        }

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          // FIXME lock for safemode
        }
      }.handle();
    } finally {
      writeUnlock();
    }
  }

  /**
   * Get registrationID for datanodes based on the namespaceID.
   *
   * @see #registerDatanode(DatanodeRegistration)
   * @return registration ID
   */
  String getRegistrationID() {
    return Storage.getRegistrationID(dir.fsImage.getStorage());
  }

  /**
   * The given node has reported in. This method should: 1) Record the
   * heartbeat, so the datanode isn't timed out 2) Adjust usage stats for future
   * block allocation
   *
   * If a substantial amount of time passed since the last datanode heartbeat
   * then request an immediate block report.
   *
   * @return an array of datanode commands
   * @throws IOException
   */
  DatanodeCommand[] handleHeartbeat(final DatanodeRegistration nodeReg,
          final long capacity, final long dfsUsed, final long remaining, final long blockPoolUsed,
          final int xceiverCount, final int xmitsInProgress, final int failedVolumes) throws IOException{
    readLock();
    try {
      final int maxTransfer = (Integer) blockManager.getMaxReplicationStreams()
              - xmitsInProgress;
      DatanodeCommand[] cmds = blockManager.getDatanodeManager().handleHeartbeat(
              nodeReg, blockPoolId, capacity, dfsUsed, remaining, blockPoolUsed,
              xceiverCount, maxTransfer, failedVolumes);
      if (cmds != null) {
        return cmds;
      }

      //check distributed upgrade
      DatanodeCommand cmd = upgradeManager.getBroadcastCommand();
      if (cmd != null) {
        return new DatanodeCommand[]{cmd};
      }
      return null;
    } finally {
      readUnlock();
    }
  }
// TODO:kamal, resource monitor
//  /**
//   * Returns whether or not there were available resources at the last check of
//   * resources.
//   *
//   * @return true if there were sufficient resources available, false otherwise.
//   */
//  private boolean nameNodeHasResourcesAvailable() {
//    return hasResourcesAvailable;
//  }
//
//  /**
//   * Perform resource checks and cache the results.
//   * @throws IOException
//   */
//  private void checkAvailableResources() throws IOException {
//    hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
//  }

//  /**
//   * Periodically calls hasAvailableResources of NameNodeResourceChecker, and if
//   * there are found to be insufficient resources available, causes the NN to
//   * enter safe mode. If resources are later found to have returned to
//   * acceptable levels, this daemon will cause the NN to exit safe mode.
//   */
//  class NameNodeResourceMonitor implements Runnable  {
//    @Override
//    public void run () {
//      try {
//        while (fsRunning) {
//          checkAvailableResources();
//          if(!nameNodeHasResourcesAvailable()) {
//            String lowResourcesMsg = "NameNode low on available disk space. ";
//            if (!isInSafeMode()) {
//              FSNamesystem.LOG.warn(lowResourcesMsg + "Entering safe mode.");
//            } else {
//              FSNamesystem.LOG.warn(lowResourcesMsg + "Already in safe mode.");
//            }
//            enterSafeMode(true);
//          }
//          try {
//            Thread.sleep(resourceRecheckInterval);
//          } catch (InterruptedException ie) {
//            // Deliberately ignore
//          }
//        }
//      } catch (Exception e) {
//        FSNamesystem.LOG.error("Exception in NameNodeResourceMonitor: ", e);
//      }
//    }
//  }
  FSImage getFSImage() {
    return dir.fsImage;
  }

  //FSEditLog getEditLog() {
//    return getFSImage().getEditLog();
//  }    
  private void checkBlock(ExtendedBlock block) throws IOException {
    if (block != null && !this.blockPoolId.equals(block.getBlockPoolId())) {
      throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
              + " - expected " + blockPoolId);
    }
  }

  @Metric({"MissingBlocks", "Number of missing blocks"})
  public long getMissingBlocksCount() {
    // not locking
    if (!isWritingNN()) {
      return -1;
    }
    try {
      return getMissingBlocksCountInternal(OperationType.GET_MISSING_BLOCKS_COUNT);
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      return -1;
    }
  }
  
  private long getMissingBlocksCountInternal(OperationType opType) throws IOException
  {
    return blockManager.getMissingBlocksCount(opType);
  }

  /**
   * Increment expired heartbeat counter.
   */
  public void incrExpiredHeartbeats() {
    expiredHeartbeats.incr();
  }

  /**
   * @see ClientProtocol#getStats()
   */
  long[] getStats() {
    final long[] stats = datanodeStatistics.getStats();
    stats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] = getUnderReplicatedBlocks();
    stats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] = getCorruptReplicaBlocks();
    try {
      stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] = getMissingBlocksCountInternal(OperationType.GET_STATS);
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] = -1;
    }
    return stats;
  }

  /**
   * Total raw bytes including non-dfs used space.
   */
  @Override // FSNamesystemMBean
  public long getCapacityTotal() {
    return datanodeStatistics.getCapacityTotal();
  }

  @Metric
  public float getCapacityTotalGB() {
    return isWritingNN() ? DFSUtil.roundBytesToGB(getCapacityTotal()) : -1;
  }

  /**
   * Total used space by data nodes
   */
  @Override // FSNamesystemMBean
  public long getCapacityUsed() {
    return datanodeStatistics.getCapacityUsed();
  }

  @Metric
  public float getCapacityUsedGB() {
    return isWritingNN() ? DFSUtil.roundBytesToGB(getCapacityUsed()) : -1;
  }

  @Override
  public long getCapacityRemaining() {
    return datanodeStatistics.getCapacityRemaining();
  }

  @Metric
  public float getCapacityRemainingGB() {
    return isWritingNN() ? DFSUtil.roundBytesToGB(getCapacityRemaining()) : -1;
  }

  /**
   * Total number of connections.
   */
  @Override // FSNamesystemMBean
  @Metric
  public int getTotalLoad() {
    return isWritingNN() ? datanodeStatistics.getXceiverCount() : -1;
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    readLock();
    try {
      return getBlockManager().getDatanodeManager().getDatanodeListForReport(
              type).size();
    } finally {
      readUnlock();
    }
  }

  DatanodeInfo[] datanodeReport(final DatanodeReportType type) throws AccessControlException {
    checkSuperuserPrivilege();
    readLock();
    try {
      final DatanodeManager dm = getBlockManager().getDatanodeManager();
      final List<DatanodeDescriptor> results = dm.getDatanodeListForReport(type);

      DatanodeInfo[] arr = new DatanodeInfo[results.size()];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = new DatanodeInfo(results.get(i));
      }
      return arr;
    } finally {
      readUnlock();
    }
  }

  /**
   * Save namespace image. This will save current namespace into fsimage file
   * and empty edits file. Requires superuser privilege and safe mode.
   *
   * @throws AccessControlException if superuser privilege is violated.
   * @throws IOException if
   */
  void saveNamespace() throws AccessControlException, IOException {
    TransactionalRequestHandler saveNamespaceHandler = new TransactionalRequestHandler(OperationType.SAVE_NAMESPACE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        checkSuperuserPrivilege();
        if (!isInSafeMode()) {
          throw new IOException("Safe mode should be turned ON "
                  + "in order to create namespace image.");
        }
        getFSImage().saveNamespace();
        LOG.info("New namespace image has been created.");
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO safemode 
      }
    };
    saveNamespaceHandler.handleWithReadLock(this);
  }

  /**
   * Enables/Disables/Checks restoring failed storage replicas if the storage
   * becomes available again. Requires superuser privilege.
   *
   * @throws AccessControlException if superuser privilege is violated.
   */
  boolean restoreFailedStorage(String arg) throws AccessControlException {
    writeLock();
    try {
      checkSuperuserPrivilege();

      // if it is disabled - enable it and vice versa.
      if (arg.equals("check")) {
        return getFSImage().getStorage().getRestoreFailedStorage();
      }

      boolean val = arg.equals("true");  // false if not
      getFSImage().getStorage().setRestoreFailedStorage(val);

      return val;
    } finally {
      writeUnlock();
    }
  }

  Date getStartTime() {
    return new Date(systemStart);
  }

  void finalizeUpgrade() throws IOException {
    checkSuperuserPrivilege();
    getFSImage().finalizeUpgrade();






  }

  /**
   * SafeModeInfo contains information related to the safe mode. <p> An instance
   * of {@link SafeModeInfo} is created when the name node enters safe mode. <p>
   * During name node startup {@link SafeModeInfo} counts the number of
   * <EntityManager>safe blocks</EntityManager>, those that have at least the
   * minimal number of replicas, and calculates the ratio of safe blocks to the
   * total number of blocks in the system, which is the size of blocks in
   * {@link FSNamesystem#blockManager}. When the ratio reaches the
   * {@link #threshold} it starts the {@link SafeModeMonitor} daemon in order to
   * monitor whether the safe mode {@link #extension} is passed. Then it leaves
   * safe mode and destroys itself. <p> If safe mode is turned on manually then
   * the number of safe blocks is not tracked because the name node is not
   * intended to leave safe mode automatically in the case.
   *
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction)
   * @see SafeModeMonitor
   */
  class SafeModeInfo {
    // configuration fields

    /**
     * Safe mode threshold condition %.
     */
    private double threshold;
    /**
     * Safe mode minimum number of datanodes alive
     */
    private int datanodeThreshold;
    /**
     * Safe mode extension after the threshold.
     */
    private int extension;
    /**
     * Min replication required by safe mode.
     */
    private int safeReplication;
    /**
     * threshold for populating needed replication queues
     */
    private double replQueueThreshold;
    // internal fields
    /**
     * Time when threshold was reached.
     *
     * <br>-1 safe mode is off <br> 0 safe mode is on, but threshold is not
     * reached yet
     */
    private long reached = -1;
    /**
     * Total number of blocks.
     */
    int blockTotal;
    /**
     * Number of safe blocks.
     */
    private int blockSafe;
    /**
     * Number of blocks needed to satisfy safe mode threshold condition
     */
    private int blockThreshold;
    /**
     * Number of blocks needed before populating replication queues
     */
    private int blockReplQueueThreshold;
    /**
     * time of the last status printout
     */
    private long lastStatusReport = 0;
    /**
     * flag indicating whether replication queues have been initialized
     */
    private boolean initializedReplQueues = false;
    /**
     * Was safemode entered automatically because available resources were low.
     */
    private boolean resourcesLow = false;

    /**
     * Creates SafeModeInfo when the name node enters automatic safe mode at
     * startup.
     *
     * @param conf configuration
     */
    private SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
              DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
      this.datanodeThreshold = conf.getInt(
              DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
              DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
      this.extension = conf.getInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
      this.safeReplication = conf.getInt(DFS_NAMENODE_REPLICATION_MIN_KEY,
              DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
      // default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
      this.replQueueThreshold =
              conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
              (float) threshold);
      this.blockTotal = 0;
      this.blockSafe = 0;
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually, or because
     * available resources are low.
     *
     * The {@link #threshold} is set to 1.5 so that it could never be reached.
     * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
     *
     * @see SafeModeInfo
     */
    private SafeModeInfo(boolean resourcesLow) {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replQueueThreshold = 1.5f; // can never be reached
      this.blockTotal = -1;
      this.blockSafe = -1;
      this.reached = -1;
      this.resourcesLow = resourcesLow;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }

    /**
     * Check if safe mode is on.
     *
     * @return true if in safe mode
     */
    private synchronized boolean isOn() throws PersistanceException {
      //TODO [lock]
      return false;
//      try {
//        assert isConsistent() : " SafeMode: Inconsistent filesystem state: "
//                + "Total num of blocks, active blocks, or "
//                + "total safe blocks don't match.";
//      } catch (IOException e) {
//        System.err.print(StringUtils.stringifyException(e));
//      }
//      return this.reached >= 0;
    }

    /**
     * Check if we are populating replication queues.
     */
    private synchronized boolean isPopulatingReplQueues() {
      return initializedReplQueues;
    }

    /**
     * Enter safe mode.
     */
    private void enter() {
      this.reached = 0;
    }

    /**
     * Leave safe mode. <p> Switch to manual safe mode if distributed upgrade is
     * required.<br> Check for invalid, under- & over-replicated blocks in the
     * end of startup.
     *
     * @throws IOException
     */
    private synchronized void leave(boolean checkForUpgrades) throws IOException, PersistanceException{
      if (checkForUpgrades) {
        // verify whether a distributed upgrade needs to be started
        boolean needUpgrade = false;
        try {
          needUpgrade = upgradeManager.startUpgrade();
        } catch (IOException e) {
          FSNamesystem.LOG.error("IOException in startDistributedUpgradeIfNeeded", e);
        }
        if (needUpgrade) {
          // switch to manual safe mode
          safeMode = new SafeModeInfo(false);
          return;
        }
      }
      // if not done yet, INITIALIZE replication queues
      if (!isPopulatingReplQueues()) {
        initializeReplQueues();
      }
      long timeInSafemode = now() - systemStart;
      NameNode.stateChangeLog.info("STATE* Leaving safe mode after "
              + timeInSafemode / 1000 + " secs.");
      NameNode.getNameNodeMetrics().setSafeModeTime((int) timeInSafemode);

      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF.");
      }
      reached = -1;
      safeMode = null;
      final NetworkTopology nt = blockManager.getDatanodeManager().getNetworkTopology();
      NameNode.stateChangeLog.info("STATE* Network topology has "
              + nt.getNumOfRacks() + " racks and "
              + nt.getNumOfLeaves() + " datanodes");
      // TODO [lock] uncomment after fixing safemode's lock
//      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
//              + blockManager.numOfUnderReplicatedBlocks() + " blocks");
    }

    /**
     * Initialize replication queues.
     *
     * @throws IOException
     */
    private synchronized void initializeReplQueues() throws IOException, PersistanceException {
      LOG.info("initializing replication queues");
      if (isPopulatingReplQueues()) {
        LOG.warn("Replication queues already initialized.");
      }
      long startTimeMisReplicatedScan = now();
      blockManager.processMisReplicatedBlocks();
      initializedReplQueues = true;
      NameNode.stateChangeLog.info("STATE* Replication Queue initialization "
              + "scan for invalid, over- and under-replicated blocks "
              + "completed in " + (now() - startTimeMisReplicatedScan)
              + " msec");
    }

    /**
     * Check whether we have reached the threshold for initializing replication
     * queues.
     */
    private synchronized boolean canInitializeReplQueues() {
      return blockSafe >= blockReplQueueThreshold;
    }

    /**
     * Safe mode can be turned off iff the threshold is reached and the
     * extension time have passed.
     *
     * @return true if can leave or false otherwise.
     */
    private synchronized boolean canLeave() {
      if (reached == 0) {
        return false;
      }
      if (now() - reached < extension) {
        reportStatus("STATE* Safe mode ON.", false);
        return false;
      }
      return !needEnter();
    }

    /**
     * There is no need to enter safe mode if DFS is empty or {@link #threshold}
     * == 0
     */
    private boolean needEnter() {
      return (threshold != 0 && blockSafe < blockThreshold)
              || (getNumLiveDataNodes() < datanodeThreshold) //              TODO:kamal, resource monitor
              //              || (!nameNodeHasResourcesAvailable())
              ;
    }

    /**
     * Check and trigger safe mode if needed.
     *
     * @throws IOException
     */
    private void checkMode() throws IOException, PersistanceException {
      if (needEnter()) {
        enter();
        // check if we are ready to INITIALIZE replication queues
        if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
          initializeReplQueues();
        }
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached


      if (!isOn() || // safe mode is off
              extension <= 0 || threshold <= 0) {  // don't need to wait
        this.leave(true); // leave safe mode
        return;
      }
      if (reached > 0) {  // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      reached = now();
      smmthread = new Daemon(new SafeModeMonitor());
      smmthread.start();
      reportStatus("STATE* Safe mode extension entered.", true);

      // check if we are ready to INITIALIZE replication queues
      if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
        initializeReplQueues();
      }
    }

    /**
     * Set total number of blocks.
     *
     * @throws IOException
     */
    private synchronized void setBlockTotal(int total) throws IOException, PersistanceException {
      this.blockTotal = total;
      this.blockThreshold = (int) (blockTotal * threshold);
      this.blockReplQueueThreshold =
              (int) (((double) blockTotal) * replQueueThreshold);
      checkMode();
    }

    /**
     * Increment number of safe blocks if current block has reached minimal
     * replication.
     *
     * @param replication current replication
     * @throws IOException
     */
    private synchronized void incrementSafeBlockCount(short replication) throws IOException, PersistanceException {
      /*
       * [JUDE] This is changed because in traditional HDFS, when the NN
       * restarts, its blockMap is empty. 'blockMap' is filled each time the DN
       * registers with the NN (via addStoredBlock method) and hence at once, on
       * the first DN registration, this DN can report to the NN to have this
       * block This would have minimum 1 replication. If 'safeReplication == 1'
       * this check will pass and 'blockSafe' variable will increment to 1
       *
       * In our case, the blockMap is not recreated if the NN restarts. When
       * finding total replica for a block, we get this value from the
       * 'triplets' table Hence, the condition 'replication == safeReplication'
       * can never be exactly equal, since after restart, we can have many
       * replica (depending on replication factor) This is just fetched from the
       * db. But in the original HDFS, after restart of NN, the actual replicas
       * are reported when all DNs have registered and reported their block
       * info. So we need to modify the condition for 'replication >=
       * safeReplication'
       */
      if ((int) replication >= safeReplication) {
        this.blockSafe++;
      }
      checkMode();
    }

    /**
     * Decrement number of safe blocks if current block has fallen below minimal
     * replication.
     *
     * @param replication current replication
     * @throws IOException
     */
    private synchronized void decrementSafeBlockCount(short replication) throws IOException, PersistanceException {
      if (replication == safeReplication - 1) {
        this.blockSafe--;
      }
      checkMode();
    }

    /**
     * Check if safe mode was entered manually or automatically (at startup, or
     * when disk space is low).
     */
    private boolean isManual() {
      return extension == Integer.MAX_VALUE && !resourcesLow;
    }

    /**
     * Set manual safe mode.
     */
    private synchronized void setManual() {
      extension = Integer.MAX_VALUE;
    }

    /**
     * Check if safe mode was entered due to resources being low.
     */
    private boolean areResourcesLow() {
      return resourcesLow;
    }

    /**
     * Set that resources are low for this instance of safe mode.
     */
    private void setResourcesLow() {
      resourcesLow = true;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() {
      if (reached < 0) {
        return "Safe mode is OFF.";
      }
      String leaveMsg = "";
      if (areResourcesLow()) {
        leaveMsg = "Resources are low on NN. Safe mode must be turned off manually";
      } else {
        leaveMsg = "Safe mode will be turned off automatically";
      }
      if (isManual()) {
//          TODO: kamal, upgrade manager
//        if(upgradeManager.getUpgradeState())
//          return leaveMsg + " upon completion of " + 
//            "the distributed upgrade: upgrade progress = " + 
//            upgradeManager.getUpgradeStatus() + "%";
        leaveMsg = "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off";
      }

      if (blockTotal < 0) {
        return leaveMsg + ".";
      }

      int numLive = getNumLiveDataNodes();
      String msg = "";
      if (reached == 0) {
        if (blockSafe < blockThreshold) {
          msg += String.format(
                  "The reported blocks %d needs additional %d"
                  + " blocks to reach the threshold %.4f of total blocks %d.",
                  blockSafe, (blockThreshold - blockSafe), threshold, blockTotal);
        }
        if (numLive < datanodeThreshold) {
          if (!"".equals(msg)) {
            msg += "\n";
          }
          msg += String.format(
                  "The number of live datanodes %d needs an additional %d live "
                  + "datanodes to reach the minimum number %d.",
                  numLive, datanodeThreshold - numLive, datanodeThreshold);
        }
        msg += " " + leaveMsg;
      } else {
        msg = String.format("The reported blocks %d has reached the threshold"
                + " %.4f of total blocks %d.", blockSafe, threshold,
                blockTotal);

        if (datanodeThreshold > 0) {
          msg += String.format(" The number of live datanodes %d has reached "
                  + "the minimum number %d.",
                  numLive, datanodeThreshold);
        }
        msg += " " + leaveMsg;
      }
      if (reached == 0 || isManual()) {  // threshold is not reached or manual       
        return msg + ".";
      }
      // extension period is in progress
      return msg + " in " + Math.abs(reached + extension - now()) / 1000
              + " seconds.";
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) {
      long curTime = now();
      if (!rightNow && (curTime - lastStatusReport < 20 * 1000)) {
        return;
      }
      NameNode.stateChangeLog.info(msg + " \n" + getTurnOffTip());
      lastStatusReport = curTime;
    }

    @Override
    public String toString() {
      String resText = "Current safe blocks = "
              + blockSafe
              + ". Target blocks = " + blockThreshold + " for threshold = %" + threshold
              + ". Minimal replication = " + safeReplication + ".";
      if (reached > 0) {
        resText += " Threshold was reached " + new Date(reached) + ".";
      }
      return resText;
    }

    /**
     * Checks consistency of the class state. This is costly and currently
     * called only in assert.
     */
    private boolean isConsistent() throws IOException, PersistanceException {
      if (!isWritingNN()) {
        return true;
      }

      if (blockTotal == -1 && blockSafe == -1) {
        return true; // manual safe mode
      }
      // TODO safemode
      int activeBlocks = blockManager.getActiveBlockCount(OperationType.SAFE_MODE_MONITOR);

      LOG.debug("safeBlocks: " + blockSafe + ", blockTotal: " + blockTotal + ", blocksActive: " + activeBlocks);
      return (blockTotal == activeBlocks)
              || (blockSafe >= 0 && blockSafe <= blockTotal);
    }
  }

  /**
   * Periodically check whether it is time to leave safe mode. This thread
   * starts when the threshold level is reached.
   *
   */
  class SafeModeMonitor implements Runnable {

    /**
     * interval in msec for checking safe mode: {@value}
     */
    private static final long recheckInterval = 1000;

    /**
     */
    public void run() {
      while (fsRunning && (safeMode != null && !safeMode.canLeave())) {
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
        }
      }
      if (!fsRunning) {
        LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread. ");
      } else {
        // leave safe mode and stop the monitor
        try {
          TransactionalRequestHandler handler = new TransactionalRequestHandler(OperationType.SAFE_MODE_MONITOR) {

            @Override
            public Object performTask() throws PersistanceException, IOException {
              leaveSafeMode(true);
              return null;
            }

            @Override
            public void acquireLock() throws PersistanceException, IOException {
              // TODO safemode
            }
          };
          handler.handle();
        } catch (SafeModeException es) { // should never happen
          String msg = "SafeModeMonitor may not run during distributed upgrade.";
          assert false : msg;
          throw new RuntimeException(msg, es);
        } catch (IOException e) {
          LOG.error(e);
        }
      }
      smmthread = null;
    }
  }
// @ClientProtocol

  boolean setSafeMode(final SafeModeAction action) throws IOException {
    TransactionalRequestHandler setSafemodeLeaveHandler = new TransactionalRequestHandler(OperationType.SET_SAFE_MODE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (action != SafeModeAction.SAFEMODE_GET) {
          checkSuperuserPrivilege();
          switch (action) {
            case SAFEMODE_LEAVE: // leave safe mode
              leaveSafeMode(false);
              break;
            case SAFEMODE_ENTER: // enter safe mode
              enterSafeMode(false);
              break;
          }
        }
        return isInSafeMode();
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
//        throw new UnsupportedOperationException("Safemode is now supported using storage-lock.");
        // TODO safemode
      }
    };
    return (Boolean) setSafemodeLeaveHandler.handle();
  }

  @Override
  public void checkSafeMode() throws IOException, PersistanceException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  @Override
  public boolean isInSafeMode() throws PersistanceException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return false;
    }
    return safeMode.isOn();
  }

  @Override
  public boolean isInStartupSafeMode() throws PersistanceException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return false;
    }
    return !safeMode.isManual() && safeMode.isOn();
  }

  @Override
  public boolean isPopulatingReplQueues() {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return true;
    }
    return safeMode.isPopulatingReplQueues();
  }

  @Override
  public void incrementSafeBlockCount(int replication) throws IOException, PersistanceException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return;
    }
    safeMode.incrementSafeBlockCount((short) replication);
  }

  @Override
  public void decrementSafeBlockCount(Block b) throws IOException, PersistanceException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) // mostly true
    {
      return;
    }
    safeMode.decrementSafeBlockCount((short) blockManager.countNodes(b).liveReplicas());
  }

  /**
   * Set the total number of blocks in the system.
   *
   * @throws IOException
   */
  private void setBlockTotal() throws IOException, PersistanceException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return;
    }
    safeMode.setBlockTotal((int) getCompleteBlocksTotal());
  }

  /**
   * Get the total number of blocks in the system.
   */
  @Override // FSNamesystemMBean
  @Metric
  public long getBlocksTotal() {
    try {
      return getBlocksTotalNoTx(OperationType.GET_BLOCKS_TOTAL);
    } catch (IOException ex) {
      LOG.error(ex);
    }
    return -1;
  }

  public long getBlocksTotalNoTx(OperationType opType) throws IOException {
    return blockManager.getTotalBlocks(opType);
  }

  /**
   * Get the total number of COMPLETE blocks in the system. For safe mode only
   * complete blocks are counted.
   *
   * @throws IOException
   */
  private long getCompleteBlocksTotal() throws IOException, PersistanceException {
    // Calculate number of blocks under construction
    long numUCBlocks = 0;
    readLock();
    try {
      for (Lease lease : leaseManager.getSortedLeases()) {
        for (LeasePath lPath : lease.getPaths()) {
          INode node;
          try {
            node = dir.getFileINode(lPath.getPath());
          } catch (UnresolvedLinkException e) {
            throw new AssertionError("Lease files should reside on this FS");
          }
          assert node != null : "Found a lease for nonexisting file.";
          assert node.isUnderConstruction() :
                  "Found a lease for file that is not under construction.";
          INodeFile cons = (INodeFile) node;
          assert cons.isUnderConstruction();
          if (cons.getBlocks().isEmpty()) {
            continue;
          }
          for (BlockInfo b : cons.getBlocks()) {
            if (!b.isComplete()) {
              numUCBlocks++;
            }
          }
        }
      }
      LOG.info("Number of blocks under construction: " + numUCBlocks);

      //TODO safemode
      return getBlocksTotalNoTx(OperationType.SAFE_MODE_MONITOR) - numUCBlocks;
    } finally {
      readUnlock();
    }
  }

  /**
   * Enter safe mode manually.
   *
   * @throws IOException
   */
  void enterSafeMode(boolean resourcesLow) throws IOException, PersistanceException {
    writeLock();
    try {
      // Ensure that any concurrent operations have been fully synced
      // before entering safe mode. This ensures that the FSImage
      // is entirely stable on disk as soon as we're in safe mode.
      //getEditLog().logSyncAll();
      if (!isInSafeMode()) {
        safeMode = new SafeModeInfo(resourcesLow);
        return;
      }
      if (resourcesLow) {
        safeMode.setResourcesLow();
      }
      safeMode.setManual();
      //getEditLog().logSyncAll();
      NameNode.stateChangeLog.info("STATE* Safe mode is ON. "
              + safeMode.getTurnOffTip());
    } finally {
      writeUnlock();
    }
  }

  /**
   * Leave safe mode.
   *
   * @throws IOException
   */
  void leaveSafeMode(boolean checkForUpgrades) throws IOException, PersistanceException {
    writeLock();
    try {
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF.");
        return;
      }
      if (upgradeManager.getUpgradeState()) {
        throw new SafeModeException("Distributed upgrade is in progress",
                safeMode);
      }
      safeMode.leave(checkForUpgrades);
    } finally {
      writeUnlock();
    }
  }

  String getSafeModeTip() throws PersistanceException {
    readLock();
    try {
      if (!isInSafeMode()) {
        return "";
      }
      return safeMode.getTurnOffTip();
    } finally {
      readUnlock();
    }
  }

  /**
   * Returns whether the given block is one pointed-to by a file.
   *
   * @throws IOException
   */
  private boolean isValidBlock(Block b) throws IOException, PersistanceException {
    return (blockManager.getINode(b) != null);
  }
  final UpgradeManagerNamenode upgradeManager = new UpgradeManagerNamenode(this);

  UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) throws IOException {
    return upgradeManager.distributedUpgradeProgress(action);
  }

  UpgradeCommand processDistributedUpgradeCommand(final UpgradeCommand comm) throws IOException {
    TransactionalRequestHandler upgradeHandler = new TransactionalRequestHandler(OperationType.PROCESS_DISTRIBUTED_UPGRADE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return upgradeManager.processUpgradeCommand(comm, true);
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO safemode
      }
    };
    return (UpgradeCommand) upgradeHandler.handle();
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
  }

  private FSPermissionChecker checkOwner(String path) throws AccessControlException, UnresolvedLinkException, PersistanceException {
    return checkPermission(path, true, null, null, null, null);
  }

  private FSPermissionChecker checkPathAccess(String path, FsAction access) throws AccessControlException, UnresolvedLinkException, PersistanceException {
    return checkPermission(path, false, null, null, access, null);
  }

  private FSPermissionChecker checkParentAccess(String path, FsAction access) throws AccessControlException, UnresolvedLinkException, PersistanceException {
    return checkPermission(path, false, null, access, null, null);
  }

  private FSPermissionChecker checkAncestorAccess(String path, FsAction access) throws AccessControlException, UnresolvedLinkException, PersistanceException {
    return checkPermission(path, false, access, null, null, null);
  }

  private FSPermissionChecker checkTraverse(String path) throws AccessControlException, UnresolvedLinkException, PersistanceException {
    return checkPermission(path, false, null, null, null, null);
  }

  @Override
  public void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker.checkSuperuserPrivilege(fsOwner, supergroup);
    }
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission(String, INodeDirectory, boolean, FsAction, FsAction, FsAction, FsAction)}.
   */
  private FSPermissionChecker checkPermission(String path, boolean doCheckOwner,
          FsAction ancestorAccess, FsAction parentAccess, FsAction access,
          FsAction subAccess) throws AccessControlException, UnresolvedLinkException, PersistanceException {
    FSPermissionChecker pc = new FSPermissionChecker(
            fsOwner.getShortUserName(), supergroup);
    if (!pc.isSuper) {
      dir.waitForReady();
      readLock();
      try {
        pc.checkPermission(path, dir.rootDir, doCheckOwner,
                ancestorAccess, parentAccess, access, subAccess);
      } finally {
        readUnlock();
      }
    }
    return pc;
  }

  /**
   * Check to see if we have exceeded the limit on the number of inodes.
   */
  void checkFsObjectLimit(OperationType opType) throws IOException, PersistanceException {
    if (maxFsObjects != 0
            && maxFsObjects <= dir.totalInodes() + getBlocksTotalNoTx(opType)) {
      throw new IOException("Exceeded the configured number of objects "
              + maxFsObjects + " in the filesystem.");
    }
  }

  /**
   * Get the total number of objects in the system.
   */
  long getMaxObjects() {
    return maxFsObjects;
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getFilesTotal() {
    if (!isWritingNN()) {
      return -1;
    }
    readLock();
    try {
      return this.dir.totalInodes();
    } finally {
      readUnlock();
    }
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getPendingReplicationBlocks() {
    return isWritingNN() ? blockManager.getPendingReplicationBlocksCount() : -1;
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getUnderReplicatedBlocks() {
    return isWritingNN() ? blockManager.getUnderReplicatedBlocksCount() : -1;
  }

  /**
   * Returns number of blocks with corrupt replicas
   */
  @Metric({"CorruptBlocks", "Number of blocks with corrupt replicas"})
  public long getCorruptReplicaBlocks() {
    return isWritingNN() ? blockManager.getCorruptReplicaBlocksCount() : -1;
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getScheduledReplicationBlocks() {
    return isWritingNN() ? blockManager.getScheduledReplicationBlocksCount() : -1;
  }

  @Metric
  public long getPendingDeletionBlocks() throws IOException {
    return isWritingNN() ? (Long) blockManager.getPendingDeletionBlocksCount(OperationType.GET_PENDING_DELETION_BLOCKS_COUNT) : -1;
  }

  @Metric
  public long getExcessBlocks() throws IOException {
    return isWritingNN() ? blockManager.getExcessBlocksCount(OperationType.GET_EXCESS_BLOCKS_COUNT) : -1;
  }

  @Metric
  public int getBlockCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override // FSNamesystemMBean
  public String getFSState() {
    try {
      TransactionalRequestHandler getFSStateHandler = new TransactionalRequestHandler(OperationType.GET_FS_STATE) {

        @Override
        public Object performTask() throws PersistanceException, IOException {
          return isInSafeMode() ? "safeMode" : "Operational";
        }

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          // TODO safemode
        }
      };
      return (String) getFSStateHandler.handle();
    } catch (IOException ex) {
      LOG.error(ex);
    }
    return null;
  }
  private ObjectName mbeanName;

  /**
   * Register the FSNamesystem MBean using the name
   * "hadoop:service=NameNode,name=FSNamesystemState"
   */
  private void registerMBean() {
    // We can only implement one MXBean interface, so we keep the old one.
    try {
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      mbeanName = MBeans.register("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad MBean setup", e);
    }

    LOG.info("Registered FSNamesystemState MBean");
  }

  /**
   * shutdown FSNamesystem
   */
  void shutdown() {
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
    }
  }

  @Override // FSNamesystemMBean
  public int getNumLiveDataNodes() {
    return getBlockManager().getDatanodeManager().getNumLiveDataNodes();
  }

  @Override // FSNamesystemMBean
  public int getNumDeadDataNodes() {
    return getBlockManager().getDatanodeManager().getNumDeadDataNodes();
  }

  /**
   * Sets the generation stamp for this filesystem
   */
  void setGenerationStamp(long stamp) {
    generationStamp.setStamp(stamp);
  }

  /**
   * Gets the generation stamp for this filesystem
   */
  long getGenerationStamp() {
    return generationStamp.getStamp();
  }

  /**
   * Increments, logs and then returns the stamp
   */
  private long nextGenerationStamp() throws SafeModeException, PersistanceException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException(
              "Cannot get next generation stamp", safeMode);
    }
    long gs = generationStamp.nextStamp();
    //getEditLog().logGenerationStamp(gs);
    // NB: callers sync the log
    return gs;
  }

  private INodeFile checkUCBlock(ExtendedBlock block,
          String clientName) throws IOException, PersistanceException {
    assert hasWriteLock();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot get a new generation stamp and an "
              + "access token for block " + block, safeMode);
    }

    // check stored block state
    BlockInfo storedBlock = blockManager.getStoredBlock(ExtendedBlock.getLocalBlock(block));
    if (storedBlock == null
            || storedBlock.getBlockUCState() != BlockUCState.UNDER_CONSTRUCTION) {
      throw new IOException(block
              + " does not exist or is not under Construction" + storedBlock);
    }

    // check file inode
    INodeFile file = storedBlock.getINode();
    if (file == null || !file.isUnderConstruction()) {
      throw new IOException("The file " + storedBlock
              + " belonged to does not exist or it is not under construction.");
    }

    // check lease
    assert file.isUnderConstruction();
    if (clientName == null || !clientName.equals(file.getClientName())) {
      throw new LeaseExpiredException("Lease mismatch: " + block
              + " is accessed by a non lease holder " + clientName);
    }

    return file;
  }

  /**
   * Get a new generation stamp together with an access token for a block under
   * construction
   *
   * This method is called for recovering a failed pipeline or setting up a
   * pipeline to append to a block.
   *
   * @param block a block
   * @param clientName the name of a client
   * @return a located block with a new generation stamp and an access token
   * @throws IOException if any error occurs
   */
  LocatedBlock updateBlockForPipeline(final ExtendedBlock block,
          final String clientName) throws IOException {
    TransactionalRequestHandler updateBlockForPipelineHandler = new TransactionalRequestHandler(OperationType.UPDATE_BLOCK_FOR_PIPELINE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        LocatedBlock locatedBlock;
        // check vadility of parameters
        checkUCBlock(block, clientName);

        // get a new generation stamp and an access token
        block.setGenerationStamp(nextGenerationStamp());
        locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
        blockManager.setBlockToken(locatedBlock, AccessMode.WRITE);
        // Ensure we record the new generation stamp
        //getEditLog().logSync();
        return locatedBlock;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager lm = new TransactionLockManager();
        lm.addINode(INodeLockType.READ).
                addBlock(LockType.READ, block.getBlockId());
        lm.acquireByBlock();
      }
    };
    return (LocatedBlock) updateBlockForPipelineHandler.handleWithWriteLock(this);
  }

  /**
   * Update a pipeline for a block under construction
   *
   * @param clientName the name of the client
   * @param oldblock and old block
   * @param newBlock a new block with a new generation stamp and length
   * @param newNodes datanodes in the pipeline
   * @throws IOException if any error occurs
   */
  void updatePipeline(final String clientName, final ExtendedBlock oldBlock,
          final ExtendedBlock newBlock, final DatanodeID[] newNodes)
          throws IOException, ImproperUsageException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }
    TransactionalRequestHandler updatePipelineHanlder = new TransactionalRequestHandler(OperationType.UPDATE_PIPELINE) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isInSafeMode()) {
          throw new SafeModeException("Pipeline not updated", safeMode);
        }
        assert newBlock.getBlockId() == oldBlock.getBlockId() : newBlock + " and "
                + oldBlock + " has different block identifier";
        LOG.info("updatePipeline(block=" + oldBlock
                + ", newGenerationStamp=" + newBlock.getGenerationStamp()
                + ", newLength=" + newBlock.getNumBytes()
                + ", newNodes=" + Arrays.asList(newNodes)
                + ", clientName=" + clientName
                + ")");

        updatePipelineInternal(clientName, oldBlock, newBlock, newNodes);
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        TransactionLockManager lm = new TransactionLockManager();
        lm.addINode(INodeLockType.WRITE).
                addBlock(LockType.WRITE, oldBlock.getBlockId()).
                addReplicaUc(LockType.READ);
        lm.acquireByBlock();
      }
    };
    updatePipelineHanlder.handleWithWriteLock(this);
    if (supportAppends) {
      //getEditLog().logSync();
    }
    LOG.info("updatePipeline(" + oldBlock + ") successfully to " + newBlock);
  }

  /**
   * @see updatePipeline(String, ExtendedBlock, ExtendedBlock, DatanodeID[])
   */
  private void updatePipelineInternal(String clientName, ExtendedBlock oldBlock,
          ExtendedBlock newBlock, DatanodeID[] newNodes)
          throws IOException, PersistanceException {
    assert isWritingNN();
    assert hasWriteLock();
    // check the vadility of the block and lease holder name
    final INodeFile pendingFile =
            checkUCBlock(oldBlock, clientName);
    assert pendingFile.isUnderConstruction();
    final BlockInfoUnderConstruction blockinfo = (BlockInfoUnderConstruction) pendingFile.getLastBlock();

    // check new GS & length: this is not expected
    if (newBlock.getGenerationStamp() <= blockinfo.getGenerationStamp()
            || newBlock.getNumBytes() < blockinfo.getNumBytes()) {
      String msg = "Update " + oldBlock + " (len = "
              + blockinfo.getNumBytes() + ") to an older state: " + newBlock
              + " (len = " + newBlock.getNumBytes() + ")";
      LOG.warn(msg);
      throw new IOException(msg);
    }

    // Update old block with the new generation stamp and new length
    blockinfo.setGenerationStamp(newBlock.getGenerationStamp());
    blockinfo.setNumBytes(newBlock.getNumBytes());

    if (newNodes.length > 0) {
      for (int i = 0; i < newNodes.length; i++) {
        ReplicaUnderConstruction replica = blockinfo.addExpectedReplica(newNodes[i].getStorageID(), HdfsServerConstants.ReplicaState.RBW);
        if (replica != null) {
          EntityManager.add(replica);
        }
      }
    }

    EntityManager.update(blockinfo);

    //[H]: No need to persist blocks in KTHFS.
    // persist blocks only if append is supported
//    String src = leaseManager.findPath(pendingFile);  
//    if (supportAppends) {
//      dir.persistBlocks(src, pendingFile);
//    }
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  void unprotectedChangeLease(String src, String dst, HdfsFileStatus dinfo)
          throws ImproperUsageException, PersistanceException {
    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }

    String overwrite;
    String replaceBy;
    assert hasWriteLock();

    boolean destinationExisted = true;
    if (dinfo == null) {
      destinationExisted = false;
    }

    if (destinationExisted && dinfo.isDir()) {
      Path spath = new Path(src);
      overwrite = spath.getParent().toString() + Path.SEPARATOR;
      replaceBy = dst + Path.SEPARATOR;
    } else {
      overwrite = src;
      replaceBy = dst;
    }

    leaseManager.changeLease(src, dst, overwrite, replaceBy);
  }

  /**
   * Serializes leases.
   */
  void saveFilesUnderConstruction(DataOutputStream out) throws ImproperUsageException, IOException, PersistanceException {
    // This is run by an inferior thread of saveNamespace, which holds a read
    // lock on our behalf. If we took the read lock here, we could block
    // for fairness if a writer is waiting on the lock.

    if (!isWritingNN()) {
      throw new ImproperUsageException();
    }

    synchronized (leaseManager) {
      out.writeInt(leaseManager.countPath()); // write the size

      for (Lease lease : leaseManager.getSortedLeases()) {
        for (LeasePath lPath : lease.getPaths()) {
          // verify that path exists in namespace
          INode node;
          try {
            node = dir.getFileINode(lPath.getPath());
          } catch (UnresolvedLinkException e) {
            throw new AssertionError("Lease files should reside on this FS");
          }
          if (node == null) {
            throw new IOException("saveLeases found path " + lPath.getPath()
                    + " but no matching entry in namespace.");
          }
          if (!node.isUnderConstruction()) {
            throw new IOException("saveLeases found path " + lPath.getPath()
                    + " but is not under construction.");
          }
          INodeFile cons = (INodeFile) node;
          assert cons.isUnderConstruction();
          FSImageSerialization.writeINodeUnderConstruction(out, cons, lPath.getPath());










        }
      }
    }
  }

//  TODO:Kamal, backup role removal
//  /**
//   * Register a Backup name-node, verifying that it belongs
//   * to the correct namespace, and adding it to the set of
//   * active journals if necessary.
//   * 
//   * @param bnReg registration of the new BackupNode
//   * @param nnReg registration of this NameNode
//   * @throws IOException if the namespace IDs do not match
//   */
//  void registerBackupNode(NamenodeRegistration bnReg,
//      NamenodeRegistration nnReg) throws IOException {
//    writeLock();
//    try {
//      if(getFSImage().getStorage().getNamespaceID() 
//         != bnReg.getNamespaceID())
//        throw new IOException("Incompatible namespaceIDs: "
//            + " Namenode namespaceID = "
//            + getFSImage().getStorage().getNamespaceID() + "; "
//            + bnReg.getRole() +
//            " node namespaceID = " + bnReg.getNamespaceID());
//      if (bnReg.getRole() == NamenodeRole.BACKUP) {
//        
//      }
//    } finally {
//      writeUnlock();
//    }
//  }
//  TODO:Kamal, back-up role removal
//  /**
//   * Release (unregister) backup node.
//   * <p>
//   * Find and remove the backup stream corresponding to the node.
//   * @param registration
//   * @throws IOException
//   */
//  void releaseBackupNode(NamenodeRegistration registration)
//    throws IOException {
//    writeLock();
//    try {
//      if(getFSImage().getStorage().getNamespaceID()
//         != registration.getNamespaceID())
//        throw new IOException("Incompatible namespaceIDs: "
//            + " Namenode namespaceID = "
//            + getFSImage().getStorage().getNamespaceID() + "; "
//            + registration.getRole() +
//            " node namespaceID = " + registration.getNamespaceID());
//      //getEditLog().releaseBackupStream(registration);
//    } finally {
//      writeUnlock();
//    }
//  }
  static class CorruptFileBlockInfo {

    String path;
    Block block;

    public CorruptFileBlockInfo(String p, Block b) {
      path = p;
      block = b;
    }

    public String toString() {
      return block.getBlockName() + "\t" + path;
    }
  }

  /**
   * @param path Restrict corrupt files to this portion of namespace.
   * @param startBlockAfter Support for continuation; the set of files we return
   * back is ordered by blockid; startBlockAfter tells where to start from
   * @return a list in which each entry describes a corrupt file/block
   * @throws AccessControlException
   * @throws IOException
   */
  Collection<CorruptFileBlockInfo> listCorruptFileBlocks(final String path,
          final String startBlockAfter) throws IOException {
    TransactionalRequestHandler listCorruptFileBlocksHandler = new TransactionalRequestHandler(OperationType.LIST_CORRUPT_FILE_BLOCKS) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (!isPopulatingReplQueues()) {
          throw new IOException("Cannot run listCorruptFileBlocks because "
                  + "replication queues have not been initialized.");
        }
        checkSuperuserPrivilege();
        long startBlockId = 0;
        // print a limited # of corrupt files per call
        int count = 0;
        ArrayList<CorruptFileBlockInfo> corruptFiles = new ArrayList<CorruptFileBlockInfo>();

        if (startBlockAfter != null) {
          startBlockId = Block.filename2id(startBlockAfter);
        }

        Collection<UnderReplicatedBlock> urblks = EntityManager.findList(UnderReplicatedBlock.Finder.ByLevel, blockManager.UNDER_REPLICATED_LEVEL_FOR_CORRUPTS);
        for (UnderReplicatedBlock urblk : urblks) {
          BlockInfo blk = EntityManager.find(BlockInfo.Finder.ById, urblk.getBlockId());
          INode inode = blk.getINode();
          if (inode != null && blockManager.countNodes(blk).liveReplicas() == 0) {
            String src = inode.getFullPathName();
            if (((startBlockAfter == null) || (blk.getBlockId() > startBlockId))
                    && (src.startsWith(path))) {
              corruptFiles.add(new CorruptFileBlockInfo(src, blk));
              count++;
              if (count >= DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED) {
                break;
              }
            }
          }
        }

        LOG.info("list corrupt file blocks returned: " + count);
        return corruptFiles;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // FIXME 
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };
    return (Collection<CorruptFileBlockInfo>) listCorruptFileBlocksHandler.handleWithReadLock(this);
  }

  /**
   * Create delegation token secret manager
   */
  private DelegationTokenSecretManager createDelegationTokenSecretManager(
          Configuration conf) {
    return new DelegationTokenSecretManager(conf.getLong(
            DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
            DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT),
            conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT),
            conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT),
            DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL, this);
  }

  /**
   * Returns the DelegationTokenSecretManager instance in the namesystem.
   *
   * @return delegation token secret manager object
   */
  DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return dtSecretManager;
  }

  /**
   * @param renewer
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   */
  Token<DelegationTokenIdentifier> getDelegationToken(final Text renewer)
          throws IOException {
    TransactionalRequestHandler getDelegationTokenHandler = new TransactionalRequestHandler(OperationType.GET_DELEGATION_TOKEN) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Token<DelegationTokenIdentifier> token;

        if (isInSafeMode()) {
          throw new SafeModeException("Cannot issue delegation token", safeMode);
        }
        if (!isAllowedDelegationTokenOp()) {
          throw new IOException(
                  "Delegation Token can be issued only with kerberos or web authentication");
        }
        if (dtSecretManager == null || !dtSecretManager.isRunning()) {
          LOG.warn("trying to get DT with no secret manager running");
          return null;
        }

        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String user = ugi.getUserName();
        Text owner = new Text(user);
        Text realUser = null;
        if (ugi.getRealUser() != null) {
          realUser = new Text(ugi.getRealUser().getUserName());
        }
        DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner,
                renewer, realUser);
        token = new Token<DelegationTokenIdentifier>(
                dtId, dtSecretManager);
//      long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
        //getEditLog().logGetDelegationToken(dtId, expiryTime);
        //getEditLog().logSync();
        return token;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO safemode
      }
    };
    return (Token<DelegationTokenIdentifier>) getDelegationTokenHandler.handleWithWriteLock(this);
  }

  /**
   *
   * @param token
   * @return New expiryTime of the token
   * @throws InvalidToken
   * @throws IOException
   */
  long renewDelegationToken(final Token<DelegationTokenIdentifier> token)
          throws InvalidToken, IOException {
    TransactionalRequestHandler renewDelegationTokenHanlder = new TransactionalRequestHandler(OperationType.RENEW_DELEGATION_TOKEN) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        long expiryTime;
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot renew delegation token", safeMode);
        }
        if (!isAllowedDelegationTokenOp()) {
          throw new IOException(
                  "Delegation Token can be renewed only with kerberos or web authentication");
        }
        String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
        expiryTime = dtSecretManager.renewToken(token, renewer);
        DelegationTokenIdentifier id = new DelegationTokenIdentifier();
        ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
        DataInputStream in = new DataInputStream(buf);
        id.readFields(in);
        //getEditLog().logRenewDelegationToken(id, expiryTime);
        //getEditLog().logSync();
        return expiryTime;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO safemode
      }
    };
    return (Long) renewDelegationTokenHanlder.handleWithWriteLock(this);
  }

  /**
   *
   * @param token
   * @throws IOException
   */
  void cancelDelegationToken(final Token<DelegationTokenIdentifier> token)
          throws IOException {
    TransactionalRequestHandler cancelDelegationTokenHandler = new TransactionalRequestHandler(OperationType.CANCEL_DELEGATION_TOKEN) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot cancel delegation token", safeMode);
        }
        String canceller = UserGroupInformation.getCurrentUser().getUserName();
        DelegationTokenIdentifier id = dtSecretManager.cancelToken(token, canceller);
        //getEditLog().logCancelDelegationToken(id);
        //getEditLog().logSync();
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO safemode
      }
    };
    cancelDelegationTokenHandler.handleWithWriteLock(this);
  }

  /**
   * @param out save state of the secret manager
   */
  void saveSecretManagerState(DataOutputStream out) throws IOException {
    dtSecretManager.saveSecretManagerState(out);
  }

  /**
   * @param in load the state of secret manager from input stream
   */
  void loadSecretManagerState(DataInputStream in) throws IOException {
    dtSecretManager.loadSecretManagerState(in);
  }

//  /**
//   * Log the updateMasterKey operation to edit logs
//   *
//   * @param key new delegation key.
//   */
//  public void logUpdateMasterKey(DelegationKey key) throws IOException {
//    writeLock();
//    try {
//      if (isInSafeMode()) {
//        throw new SafeModeException(
//                "Cannot log master key update in safe mode", safeMode);
//      }
//      //getEditLog().logUpdateMasterKey(key);
//    } finally {
//      writeUnlock();
//    }
//    //getEditLog().logSync();
//  }
  private void logReassignLease(String leaseHolder, String src,
          String newHolder) throws IOException {
    writeLock();
    try {
      //getEditLog().logReassignLease(leaseHolder, src, newHolder);
    } finally {
      writeUnlock();
    }
    //getEditLog().logSync();
  }

  /**
   *
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
            && (authMethod != AuthenticationMethod.KERBEROS)
            && (authMethod != AuthenticationMethod.KERBEROS_SSL)
            && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }

  /**
   * Returns authentication method used to establish the connection
   *
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
          throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  /**
   * Client invoked methods are invoked over RPC and will be in RPC call context
   * even if the client exits.
   */
  private boolean isExternalInvocation() {
    return Server.isRpcInvocation();
  }

  /**
   * Log fsck event in the audit log
   */
  void logFsckEvent(String src, InetAddress remoteAddress) throws IOException {
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
              remoteAddress,
              "fsck", src, null, null);
    }
  }

  /**
   * Register NameNodeMXBean
   */
  private void registerMXBean() {
    MBeans.register("NameNode", "NameNodeInfo", this);
  }

  /**
   * Class representing Namenode information for JMX interfaces
   */
  @Override // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override // NameNodeMXBean
  public long getUsed() {
    return this.getCapacityUsed();
  }

  @Override // NameNodeMXBean
  public long getFree() {
    return this.getCapacityRemaining();
  }

  @Override // NameNodeMXBean
  public long getTotal() {
    return this.getCapacityTotal();
  }

  @Override // NameNodeMXBean
  public String getSafemode() {
    try {
      TransactionalRequestHandler getSafemodeHandler = new TransactionalRequestHandler(OperationType.GET_SAFE_MODE) {

        @Override
        public Object performTask() throws PersistanceException, IOException {
          if (!isInSafeMode()) {
            return "";
          }
          return "Safe mode is ON." + getSafeModeTip();
        }

        @Override
        public void acquireLock() throws PersistanceException, IOException {
          // TODO safemode
        }
      };
      return (String) getSafemodeHandler.handle();
    } catch (IOException ex) {
      LOG.error(ex);
    }
    return null;
  }

  @Override // NameNodeMXBean
  public boolean isUpgradeFinalized() {
    return this.getFSImage().isUpgradeFinalized();
  }

  @Override // NameNodeMXBean
  public long getNonDfsUsedSpace() {
    return datanodeStatistics.getCapacityUsedNonDFS();
  }

  @Override // NameNodeMXBean
  public float getPercentUsed() {
    return datanodeStatistics.getCapacityUsedPercent();
  }

  @Override // NameNodeMXBean
  public long getBlockPoolUsedSpace() {
    return datanodeStatistics.getBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentBlockPoolUsed() {
    return datanodeStatistics.getPercentBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentRemaining() {
    return datanodeStatistics.getCapacityRemainingPercent();
  }

  @Override // NameNodeMXBean
  public long getTotalBlocks() {
    return getBlocksTotal();
  }

  @Override // NameNodeMXBean
  @Metric
  public long getTotalFiles() {
    return isWritingNN() ? getFilesTotal() : -1;
  }

  @Override // NameNodeMXBean
  public long getNumberOfMissingBlocks() {
    return getMissingBlocksCount();
  }

  @Override // NameNodeMXBean
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of live node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getLiveNodes() {
    final Map<String, Map<String, Object>> info =
            new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
    for (DatanodeDescriptor node : live) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      innerinfo.put("usedSpace", getDfsUsed(node));
      innerinfo.put("adminState", node.getAdminState().toString());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Map<String, Object>> info =
            new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(null, dead, true);
    for (DatanodeDescriptor node : dead) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      innerinfo.put("decommissioned", node.isDecommissioned());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decomisioning node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Map<String, Object>> info =
            new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> decomNodeList = blockManager.getDatanodeManager().getDecommissioningNodes();
    for (DatanodeDescriptor node : decomNodeList) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("underReplicatedBlocks", node.decommissioningStatus.getUnderReplicatedBlocks());
      innerinfo.put("decommissionOnlyReplicas", node.decommissioningStatus.getDecommissionOnlyReplicas());
      innerinfo.put("underReplicateInOpenFiles", node.decommissioningStatus.getUnderReplicatedInOpenFiles());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (System.currentTimeMillis() - alivenode.getLastUpdate()) / 1000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
  }

  @Override  // NameNodeMXBean
  public String getClusterId() {
    return dir.fsImage.getStorage().getClusterID();
  }

  @Override  // NameNodeMXBean
  public String getBlockPoolId() {
    //return blockPoolId; //[thesis]
    return "h4ck3d-810ck-p001";
  }

  /**
   * @return the block manager.
   */
  public BlockManager getBlockManager() {
    return blockManager;
  }

  /**
   * Added for acquiring locks in KTHFS.
   */
  public FSDirectory getFsDirectory() {
    return this.dir;
  }
}
