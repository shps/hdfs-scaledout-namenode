#!/bin/bash
. set-env.sh
$HADOOP_HDFS_HOME/bin/hdfs dfs $@ 
