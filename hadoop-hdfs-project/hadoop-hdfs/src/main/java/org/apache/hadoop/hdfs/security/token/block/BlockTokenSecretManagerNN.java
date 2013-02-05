/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.security.token.block;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.BlockTokenKeyDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;


/**
 * This class persists the secret keys (used for Token generation) in NDB 
 * It should only be used by KTHFS Namenodes 
 * 
 * BlockTokenSecretManager can be instantiated in 2 modes, master mode and slave
 * mode. Master can generate new block keys and export block keys to slaves,
 * while slaves can only import and use block keys received from master. Both
 * master and slave can generate and verify block tokens. Typically, master mode
 * is used by NN and slave mode is used by DN.
 */
@InterfaceAudience.Private
public class BlockTokenSecretManagerNN extends
    SecretManager<BlockTokenIdentifier> {
  public static final Log LOG = LogFactory
      .getLog(BlockTokenSecretManagerNN.class);

  private final boolean isMaster;
  /**
   * keyUpdateInterval is the interval that NN updates its block keys. It should
   * be set long enough so that all live DN's and Balancer should have sync'ed
   * their block keys with NN at least once during each interval.
   */
  private final long keyUpdateInterval;
  private volatile long tokenLifetime;
  private int serialNo = new SecureRandom().nextInt();
  private BlockKey currentKey;
  private BlockKey nextKey;

  public static enum AccessMode {
    READ, WRITE, COPY, REPLACE
  };

  /**
   * Constructor
   * 
   * @param isMaster
   * @param keyUpdateInterval
   * @param tokenLifetime
   * @throws IOException
   */
  public BlockTokenSecretManagerNN(boolean isMaster, long keyUpdateInterval,
          long tokenLifetime, boolean isLeader) throws IOException {
    this.isMaster = isMaster;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenLifetime = tokenLifetime;


    if (isLeader) {
      // TODO[Hooman]: Since Master is keeping the serialNo locally, so whenever
      // A namenode crashes it should remove all keys from the database.
      
      // generate new keys
      generateKeys();
    } else {
      retrieveCurrentKeyAndNextKey();
    }
  }

  /** Initialize block keys */
  // Only called by the constructor which is called by BlockManager's activate() method
  // This is a one time operation when the namenode restarts
  private synchronized void generateKeys() throws IOException { //CHANGED
    if (!isMaster) {
      return;
    }
    /*
     * Need to set estimated expiry dates for currentKey and nextKey so that if
     * NN crashes, DN can still expire those keys. NN will stop using the newly
     * generated currentKey after the first keyUpdateInterval, however it may
     * still be used by DN and Balancer to generate new tokens before they get a
     * chance to sync their keys with NN. Since we require keyUpdInterval to be
     * long enough so that all live DN's and Balancer will sync their keys with
     * NN at least once during the period, the estimated expiry date for
     * currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
     * Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
     * more.
     */
    serialNo++;
    currentKey = new BlockKey(serialNo, System.currentTimeMillis() + 2
            * keyUpdateInterval + tokenLifetime, generateSecret());
    currentKey.setKeyType(BlockKey.CURR_KEY);
    serialNo++;
    nextKey = new BlockKey(serialNo, System.currentTimeMillis() + 3
            * keyUpdateInterval + tokenLifetime, generateSecret());
    nextKey.setKeyType(BlockKey.NEXT_KEY);


    TransactionalRequestHandler generateKeysHandler = new TransactionalRequestHandler(OperationType.ADD_BLOCK_TOKENS) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        EntityManager.add(currentKey);
        EntityManager.add(nextKey);
        return null;
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // No lock is required.
      }
    };
    generateKeysHandler.handle();
  }

  /** Export block keys, only to be used in master mode */
  // Keys are to be exported to Datanodes when required
  // Its usually exported during datanode heart beats and when keys are to be updated (This updation is monitored by the HeartBeat monitor)
  // This method simply returns the current key and other 'unexpired' keys from the database
  public synchronized ExportedBlockKeys exportKeys() { //CHANGED
    if (!isMaster) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exporting access keys");
    }
    BlockKey[] allKeys = null;
    try {
      allKeys = (BlockKey[]) getAllKeys().toArray();
    } catch (IOException ex) {
      LOG.error(ex);
      // TODO[Hooman]: throw exception to the caller.
    }
    return new ExportedBlockKeys(
            true,
            keyUpdateInterval,
            tokenLifetime,
            getKeyByType(BlockKey.CURR_KEY),
            allKeys);
  }

  private synchronized void removeExpiredKeys() throws IOException { //CHANGED

    TransactionalRequestHandler removeKeyHandler = new TransactionalRequestHandler(OperationType.REMOVE_BLOCK_KEY) {

      long now = System.currentTimeMillis();

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        int keyId = (Integer) params[0];
        TransactionLockManager tlm = new TransactionLockManager();
        tlm.addBlockKeyLockById(TransactionLockManager.LockType.WRITE, keyId).acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        int keyId = (Integer) params[0];
        BlockKey key = EntityManager.find(BlockKey.Finder.ById, keyId);
        if (key != null) {
          if (key.getExpiryDate() < now) {
            EntityManager.remove(key);
          }
        }
        return null;
      }
    };

    List<BlockKey> allKeys = getAllKeys();
    for (BlockKey key : allKeys) {
      removeKeyHandler.setParams(key.getKeyId()).handle();
    }
  }

  private List<BlockKey> getAllKeys() throws IOException {
    return (List<BlockKey>) new LightWeightRequestHandler(OperationType.GET_ALL_BLOCK_TOKENS) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        BlockTokenKeyDataAccess da = (BlockTokenKeyDataAccess) StorageFactory.getDataAccess(BlockTokenKeyDataAccess.class);
        List<BlockKey> allKeys = da.findAll();
        return allKeys;
      }
    }.handle();
  }
  /**
   * Update block keys if update time > update interval.
   * @return true if the keys are updated.
   */
  // This method decides whether the current key should be updated
  // If so, update the current key to the next key and generate a new current key
  // Also removes any expired keys
  public boolean updateKeys(boolean isLeader, final long updateTime) throws IOException {
    if (updateTime > keyUpdateInterval) {
      return updateKeys(isLeader);
    }
    return false;
  }

  /**
   * Update block keys, only to be used in master mode
   */
  synchronized boolean updateKeys(boolean isLeader) throws IOException { //CHANGED
    if (!isMaster)
      return false;
    
    // allow for modification in db only by the leader
    if(isLeader) {
      
      LOG.info("Updating block keys");
      removeExpiredKeys();
      
      new TransactionalRequestHandler(OperationType.UPDATE_BLOCK_KEYS) {
        
        @Override
        public void acquireLock() throws PersistanceException, IOException {
          TransactionLockManager tlm = new TransactionLockManager();
          tlm.addBlockKeyLockByType(TransactionLockManager.LockType.WRITE, BlockKey.CURR_KEY).
                  addBlockKeyLockByType(TransactionLockManager.LockType.WRITE, BlockKey.NEXT_KEY).
                  acquire();
        }

        @Override
        public Object performTask() throws PersistanceException, IOException {
          // set final expiry date of retiring currentKey
          // also modifying this key to mark it as 'simple key' instead of 'current key'
          BlockKey currentKeyFromDB = EntityManager.find(BlockKey.Finder.ByType, BlockKey.CURR_KEY);
          currentKeyFromDB.setExpiryDate(System.currentTimeMillis() + keyUpdateInterval + tokenLifetime);
          currentKeyFromDB.setKeyType(BlockKey.SIMPLE_KEY);
          EntityManager.update(currentKeyFromDB);
          // after above update, we only have a key marked as 'next key'
          // the 'next key' becomes the 'current key'
          // update the estimated expiry date of new currentKey
          BlockKey nextKeyFromDB = EntityManager.find(BlockKey.Finder.ByType, BlockKey.NEXT_KEY);
          currentKey = new BlockKey(nextKeyFromDB.getKeyId(), System.currentTimeMillis()
                  + 2 * keyUpdateInterval + tokenLifetime, nextKeyFromDB.getKey());
          currentKey.setKeyType(BlockKey.CURR_KEY);
          EntityManager.update(currentKey);

          // generate a new nextKey
          serialNo++;
          nextKey = new BlockKey(serialNo, System.currentTimeMillis() + 3
                  * keyUpdateInterval + tokenLifetime, generateSecret());
          nextKey.setKeyType(BlockKey.NEXT_KEY);
          EntityManager.add(nextKey);
          return null;
        }
      }.handle();
    }
    else {
      retrieveCurrentKeyAndNextKey();
    }
    
    return true;
  }
  
  private void retrieveCurrentKeyAndNextKey() throws IOException {
    currentKey = getKeyByType(BlockKey.CURR_KEY);
    nextKey = getKeyByType(BlockKey.NEXT_KEY);
  }

  private BlockKey getKeyByType(final short keyType) {
    try {
      return (BlockKey) new LightWeightRequestHandler(OperationType.GET_KEY_BY_TYPE) {

        @Override
        public Object performTask() throws PersistanceException, IOException {
          BlockTokenKeyDataAccess da = (BlockTokenKeyDataAccess) StorageFactory.getDataAccess(BlockTokenKeyDataAccess.class);
          return da.findByKeyType(keyType);
        }
      }.handle();
    } catch (IOException ex) {
      LOG.error(ex);
    }
    return null;
  }
  
  /** Generate an block token for current user */
  public Token<BlockTokenIdentifier> generateToken(ExtendedBlock block,
      EnumSet<BlockTokenSecretManager.AccessMode> modes) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userID = (ugi == null ? null : ugi.getShortUserName());
    return generateToken(userID, block, modes); 
  }

  /** Generate a block token for a specified user */
  public Token<BlockTokenIdentifier> generateToken(String userId,
      ExtendedBlock block, EnumSet<BlockTokenSecretManager.AccessMode> modes) throws IOException {
    BlockTokenIdentifier id = new BlockTokenIdentifier(userId, block
        .getBlockPoolId(), block.getBlockId(), modes);
    return new Token<BlockTokenIdentifier>(id, this);
  }

  /**
   * Check if access should be allowed. userID is not checked if null. This
   * method doesn't check if token password is correct. It should be used only
   * when token password has already been verified (e.g., in the RPC layer).
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, AccessMode mode) throws InvalidToken {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking access for user=" + userId + ", block=" + block
          + ", access mode=" + mode + " using " + id.toString());
    }
    if (userId != null && !userId.equals(id.getUserId())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't belong to user " + userId);
    }
    if (!id.getBlockPoolId().equals(block.getBlockPoolId())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't apply to block " + block);
    }
    if (id.getBlockId() != block.getBlockId()) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't apply to block " + block);
    }
    if (isExpired(id.getExpiryDate())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " is expired.");
    }
    if (!id.getAccessModes().contains(mode)) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't have " + mode + " permission");
    }
  }

  /** Check if access should be allowed. userID is not checked if null */
  public void checkAccess(Token<BlockTokenIdentifier> token, String userId,
      ExtendedBlock block, AccessMode mode) throws InvalidToken {
    BlockTokenIdentifier id = new BlockTokenIdentifier();
    try {
      id.readFields(new DataInputStream(new ByteArrayInputStream(token
          .getIdentifier())));
    } catch (IOException e) {
      throw new InvalidToken(
          "Unable to de-serialize block token identifier for user=" + userId
              + ", block=" + block + ", access mode=" + mode);
    }
    checkAccess(id, userId, block, mode);
    if (!Arrays.equals(retrievePassword(id), token.getPassword())) {
      throw new InvalidToken("Block token with " + id.toString()
          + " doesn't have the correct token password");
    }
  }

  private static boolean isExpired(long expiryDate) {
    return System.currentTimeMillis() > expiryDate;
  }

  /**
   * check if a token is expired. for unit test only. return true when token is
   * expired, false otherwise
   */
  static boolean isTokenExpired(Token<BlockTokenIdentifier> token)
      throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    long expiryDate = WritableUtils.readVLong(in);
    return isExpired(expiryDate);
  }

  /** set token lifetime. */
  public void setTokenLifetime(long tokenLifetime) {
    this.tokenLifetime = tokenLifetime;
  }
  public long getTokenLifetime() {
    return tokenLifetime;
  }
  /**
   * Create an empty block token identifier
   * 
   * @return a newly created empty block token identifier
   */
  @Override
  public BlockTokenIdentifier createIdentifier() {
    return new BlockTokenIdentifier();
  }

  /**
   * Create a new password/secret for the given block token identifier.
   * 
   * @param identifier
   *          the block token identifier
   * @return token password/secret
   */
  @Override
  protected byte[] createPassword(BlockTokenIdentifier identifier) {
    BlockKey key = null;
    synchronized (this) {
      key = getKeyByType(BlockKey.CURR_KEY);
      if (key == null) {
        throw new IllegalStateException("currentKey hasn't been initialized.");
      }

      identifier.setExpiryDate(System.currentTimeMillis() + tokenLifetime);
      identifier.setKeyId(key.getKeyId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Generating block token for " + identifier.toString());
      }

      return createPassword(identifier.getBytes(), key.getKey());
    }
  }

  /**
   * Look up the token password/secret for the given block token identifier.
   * 
   * @param identifier
   *          the block token identifier to look up
   * @return token password/secret as byte[]
   * @throws InvalidToken
   */
  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier)
      throws InvalidToken {
    if (isExpired(identifier.getExpiryDate())) {
      throw new InvalidToken("Block token with " + identifier.toString()
          + " is expired.");
    }

    BlockKey key = null;
    synchronized (this) {
      try {
        key = getKeyById(identifier.getKeyId());
      } catch (IOException ex) {
        LOG.error(ex);
      }
    }
    if (key == null) {
      throw new InvalidToken("Can't re-compute password for "
          + identifier.toString() + ", since the required block key (keyID="
          + identifier.getKeyId() + ") doesn't exist.");
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }
  
  private BlockKey getKeyById(final int keyId) throws IOException {
    return (BlockKey) new LightWeightRequestHandler(OperationType.GET_KEY_BY_ID) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        BlockTokenKeyDataAccess da = (BlockTokenKeyDataAccess) StorageFactory.getDataAccess(BlockTokenKeyDataAccess.class);
        return da.findByKeyId(keyId);
      }
    }.handle();
  }
}
