#!/bin/bash

# You have to build mysql-cluster from source. Do this by executing:
# cd $ROOT
# cmake .  -DWITH_NDB_TEST=ON -LH
# make 
# cd storage/ndb
# make
# It often fails on building java code. Just re-run make after killing the shell.
# 
export NDB_CONNECTSTRING="cloud11.sics.se:1186"
#-create_table
if [ $# -ne 3 ] ; then
echo "Usage: $0 number_threads warmup_time exec_time"
exit 1
fi 
/root/opt/mysql-cluster-gpl-7.2.8/storage/ndb/test/ndbapi/flexAsynch -t $1 -warmup_time $2 -execution_time $3 -cooldown_time 1000 -load_factor 50 
#/root/opt/mysql-cluster-gpl-7.2.8/storage/ndb/test/ndbapi/flexAsynch -t 64 -warmup_time 2000 -execution_time 20000 -cooldown_time 1000 -load_factor 50 -simple
#/root/opt/mysql-cluster-gpl-7.2.8/storage/ndb/test/ndbapi/flexAsynch -t 128 -l 1 -warmup_time 3000 -execution_time 30000 -cooldown_time 1000 -s 5 -load_factor 50 -read 
