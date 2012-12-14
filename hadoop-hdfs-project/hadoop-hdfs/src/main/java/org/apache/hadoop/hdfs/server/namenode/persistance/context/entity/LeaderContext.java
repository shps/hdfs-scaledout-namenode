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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
// [S]
// FIXME this chache is not generic. 
// Design a generic chache that works for all 
// type of transaction contexts
//
// should not wirte context again agian for every 
// table
//
public class LeaderContext extends EntityContext<Leader> {

    private Map<Long, Leader> allReadLeaders = new HashMap<Long, Leader>();
    private Map<Long, Leader> modifiedLeaders = new HashMap<Long, Leader>();
    private Map<Long, Leader> removedLeaders = new HashMap<Long, Leader>();
    private Map<Long, Leader> newLeaders = new HashMap<Long, Leader>();
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
    }

    @Override
    public int count(CounterType<Leader> counter, Object... params) throws PersistanceException {
        Leader.Counter lCounter = (Leader.Counter) counter;

        switch (lCounter) {
            case AllById:
                log("count-all-leaders", CacheHitState.LOSS);
                aboutToAccessStorage();
                return dataAccess.countAllById();
        }

        throw new RuntimeException(UNSUPPORTED_COUNTER);

    }

    @Override
    public Leader find(FinderType<Leader> finder, Object... params) throws PersistanceException {
        Leader.Finder lFinder = (Leader.Finder) finder;
        Leader leader;

        switch (lFinder) {
            case ById:
                Long id = (Long) params[0];
                if (allReadLeaders.containsKey(id)) {
                    log("find-leader-by-id", CacheHitState.HIT, new String[]{
                                "leaderId", Long.toString(id)
                            });
                    leader = allReadLeaders.get(id);
                } else {
                    log("find-leader-by-id", CacheHitState.LOSS, new String[]{
                                "leaderId", Long.toString(id)
                            });
                    aboutToAccessStorage();
                    leader = dataAccess.findById(id);
                    if (leader != null) {
                        allReadLeaders.put(leader.getId(), leader);
                    }
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

                return leaders;

            case AllByIDLT:
                long id = (Long) params[0];
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
                
                return leaders;

            case All:
                aboutToAccessStorage();
                leaders = dataAccess.findAll();

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

                return leaders;
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

        log("updated-leader", CacheHitState.NA, new String[]{
                    "id", Long.toString(leader.getId()), "hostName", leader.getHostName(), "counter", Long.toString(leader.getCounter()),
                    "timeStamp", Long.toString(leader.getTimeStamp())
                });
    }
}
