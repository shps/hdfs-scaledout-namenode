#!/bin/bash

. ./set-env.sh 

username=`cut -f 1 -d " " username`
dist=`cut -f 1 -d " " distribution_folder_name`
rootDir="/home/$username/$dist"

HDFS=$HADOOP_COMMON_HOME/sbin/hadoop-daemon.sh

echo "Starting to format namenode ..."
echo "$HADOOP_HDFS_HOME/bin/hdfs namenode -format -clusterid 1000"
$HADOOP_HDFS_HOME/bin/hdfs namenode -format -clusterid 1000

exit $result
