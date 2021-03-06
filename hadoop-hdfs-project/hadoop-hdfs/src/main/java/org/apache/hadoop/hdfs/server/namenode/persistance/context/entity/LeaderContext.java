/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.CounterType;
import org.apache.hadoop.hdfs.server.namenode.FinderType;
import org.apache.hadoop.hdfs.server.namenode.Leader;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.context.TransactionContextException;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaderDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author salman
 */
public class LeaderContext extends EntityContext<Leader> {

  private Map<Long, Leader> allReadLeaders = new HashMap<Long, Leader>();
  private Map<Long, Leader> modifiedLeaders = new HashMap<Long, Leader>();
  private Map<Long, Leader> removedLeaders = new HashMap<Long, Leader>();
  private Map<Long, Leader> newLeaders = new HashMap<Long, Leader>();
  private boolean allRead = false;
  private LeaderDataAccess dataAccess;

  public LeaderContext(LeaderDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(Leader leader) throws PersistanceException {
    if (removedLeaders.containsKey(leader.getId())) {
      throw new TransactionContextException("Removed leader passed for persistance");
    }

    newLeaders.put(leader.getId(), leader);

    //put new leader in the AllReadLeaders List
    //for the future operatrions in the same transaction
    //
    allReadLeaders.put(leader.getId(), leader);

    log("added-leader", CacheHitState.NA,
            new String[]{
      "id", Long.toString(leader.getId()), "hostName", leader.getHostName(), "counter", Long.toString(leader.getCounter()),
      "timeStamp", Long.toString(leader.getTimeStamp())
    });
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    modifiedLeaders.clear();
    removedLeaders.clear();
    newLeaders.clear();
    allReadLeaders.clear();
    allRead = false;
  }

  @Override
  public int count(CounterType<Leader> counter, Object... params) throws PersistanceException {
    Leader.Counter lCounter = (Leader.Counter) counter;

    switch (lCounter) {
      case All:
        log("count-all-leaders", CacheHitState.LOSS);
        if (allRead) {
          return allReadLeaders.size();
        } else {
          aboutToAccessStorage();
          //allRead = true; //[S] commented this line. does not make sense
          return dataAccess.countAll();
        }
      case AllPredecessors:
        log("count-all-predecessor-leaders", CacheHitState.LOSS);
        Long id = (Long) params[0];
        if (allRead) {
          return findPredLeadersFromMapping(id).size();
        } else {
          aboutToAccessStorage();
          return dataAccess.countAllPredecessors(id);
        }
      case AllSuccessors:
        log("count-all-Successor-leaders", CacheHitState.LOSS);
        id = (Long) params[0];
        if (allRead) {
          return findSuccLeadersFromMapping(id).size();
        } else {
          aboutToAccessStorage();
          return dataAccess.countAllSuccessors(id);
        }
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);

  }

  private List<Leader> findPredLeadersFromMapping(long id) {
    List<Leader> preds = new ArrayList<Leader>();
    for (Leader leader : allReadLeaders.values()) {
      if (leader.getId() < id) {
        preds.add(leader);
      }
    }

    return preds;
  }

  private List<Leader> findLeadersWithGreaterCounterFromMapping(long counter) {
    List<Leader> greaterLeaders = new ArrayList<Leader>();
    for (Leader leader : allReadLeaders.values()) {
      if (leader.getCounter() > counter) {
        greaterLeaders.add(leader);
      }
    }

    return greaterLeaders;
  }

  private List<Leader> findSuccLeadersFromMapping(long id) {
    List<Leader> preds = new ArrayList<Leader>();
    for (Leader leader : allReadLeaders.values()) {
      if (leader.getId() > id) {
        preds.add(leader);
      }
    }

    return preds;
  }

  @Override
  public Leader find(FinderType<Leader> finder, Object... params) throws PersistanceException {
    Leader.Finder lFinder = (Leader.Finder) finder;
    Leader leader;

    switch (lFinder) {
      case ById:
        Long id = (Long) params[0];
        int partitionKey = (Integer) params[1];
        if (allRead || allReadLeaders.containsKey(id)) {
          log("find-leader-by-id", CacheHitState.HIT, new String[]{"leaderId", Long.toString(id)});
          leader = allReadLeaders.get(id);
        } else {
          log("find-leader-by-id", CacheHitState.LOSS, new String[]{
            "leaderId", Long.toString(id)
          });
          aboutToAccessStorage();
          leader = dataAccess.findByPkey(id, partitionKey);
          allReadLeaders.put(id, leader);
        }
        return leader;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<Leader> findList(FinderType<Leader> finder, Object... params) throws PersistanceException {
    Leader.Finder lFinder = (Leader.Finder) finder;
    Collection<Leader> leaders = null;

    switch (lFinder) {
      case AllByCounterGTN:
        long counter = (Long) params[0];
        // we are looking for range of row
        // not a good idea for running a range query on the cache
        // just get the damn rows from ndb
        if (allRead) {
          leaders = findLeadersWithGreaterCounterFromMapping(counter);
        } else {
          aboutToAccessStorage();
          leaders = dataAccess.findAllByCounterGT(counter);

          // put all read leaders in the cache
          if (leaders != null) {
            Iterator itr = leaders.iterator();
            while (itr.hasNext()) {
              Leader leader = (Leader) itr.next();
              if (!allReadLeaders.containsKey(leader.getId())) {
                allReadLeaders.put(leader.getId(), leader);
              }
            }
          }
        }

        return leaders;

      case AllByIDLT:
        long id = (Long) params[0];
        if (allRead) {
          leaders = findPredLeadersFromMapping(id);
        } else {
          aboutToAccessStorage();
          leaders = dataAccess.findAllByIDLT(id);

          // put all read leaders in the cache
          if (leaders != null) {
            Iterator itr = leaders.iterator();
            while (itr.hasNext()) {
              Leader leader = (Leader) itr.next();
              if (!allReadLeaders.containsKey(leader.getId())) {
                allReadLeaders.put(leader.getId(), leader);
              }
            }
          }
        }
        return leaders;

      case All:
        if (allRead) {
          leaders = allReadLeaders.values();
        } else {
          aboutToAccessStorage();
          leaders = dataAccess.findAll();
          allRead = true;
          // put all read leaders in the cache
          if (leaders != null) {
            Iterator itr = leaders.iterator();
            while (itr.hasNext()) {
              Leader leader = (Leader) itr.next();
              if (!allReadLeaders.containsKey(leader.getId())) {
                allReadLeaders.put(leader.getId(), leader);
              }
            }
          }
        }
        return new ArrayList(leaders);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare() throws StorageException {
    dataAccess.prepare(removedLeaders.values(), newLeaders.values(), modifiedLeaders.values());
  }

  @Override
  public void remove(Leader leader) throws PersistanceException {
    removedLeaders.put(leader.getId(), leader);

    if (allReadLeaders.containsKey(leader.getId())) {
      allReadLeaders.remove(leader.getId());
    }

    log("removed-leader", CacheHitState.NA, new String[]{
      "id", Long.toString(leader.getId()), "hostName", leader.getHostName(), "counter", Long.toString(leader.getCounter()),
      "timeStamp", Long.toString(leader.getTimeStamp())
    });
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void update(Leader leader) throws PersistanceException {
    if (removedLeaders.containsKey(leader.getId())) {
      throw new TransactionContextException("Trying to update a removed leader record");
    }

    modifiedLeaders.put(leader.getId(), leader);

    // update the allReadLeaders cache
    if (allReadLeaders.containsKey(leader.getId())) {
      allReadLeaders.remove(leader.getId());
    }
    allReadLeaders.put(leader.getId(), leader);

    log("updated-leader", CacheHitState.NA, new String[]{
      "id", Long.toString(leader.getId()), "hostName", leader.getHostName(), "counter", Long.toString(leader.getCounter()),
      "timeStamp", Long.toString(leader.getTimeStamp())
    });
  }
}
