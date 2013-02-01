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

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * Key used for generating and verifying block tokens
 */
@InterfaceAudience.Private
public class BlockKey extends DelegationKey {
  
  public static enum Finder implements org.apache.hadoop.hdfs.server.namenode.FinderType<BlockKey> {

    ById, ByType, All;

    @Override
    public Class getType() {
      return BlockKey.class;
    }
  }
  
  // KTHFS
  public static final short CURR_KEY = 0;
	public static final short NEXT_KEY = 1;
	public static final short SIMPLE_KEY = -1;
  // To specify if this key is currentKey or nextKey.
  private short keyType;

  public BlockKey() {
    super();
  }

  public BlockKey(int keyId, long expiryDate, SecretKey key) {
    super(keyId, expiryDate, key);
  }
  
  public void setKeyType(short keyType)
  {
    this.keyType = keyType;
  }
  
  public short getKeyType()
  {
    return this.keyType;
  }
}
