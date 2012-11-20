#!/bin/bash

# This script sets the required environment variables for KTHFS
# Input parameter as the working directory
if [ -z $1 ]; then
	echo "Error: No home directory is specified."
	exit
fi

export HADOOP_DEV_HOME=$1
export HADOOP_COMMON_HOME=$HADOOP_DEV_HOME/hadoop-common-0.24.0-SNAPSHOT
export HADOOP_HDFS_HOME=$HADOOP_DEV_HOME/hadoop-hdfs-0.24.0-SNAPSHOT
export HADOOP_CONF_DIR=$HADOOP_DEV_HOME/conf
export HADOOP_HOME=$HADOOP_DEV_HOME
export JAVA_HOME=/usr
export KTHFS_DATABASE=wasif
export KTHFS_CONN_STR=cloud11.sics.se
export LD_LIBRARY_PATH=$HADOOP_DEV_HOME/conf
