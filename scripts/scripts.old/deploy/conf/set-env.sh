#!/bin/bash
DIST=dist
username=kthfs
#current_dir=$(pwd)
#export PATH=$PATH:$current_dir
export HADOOP_HDFS_HOME="/home/${username}/${DIST}/hadoop-hdfs-0.24.0-SNAPSHOT"
export HADOOP_COMMON_HOME="/home/${username}/${DIST}/hadoop-common-0.24.0-SNAPSHOT"
export LD_LIBRARY_PATH="/home/${username}/${DIST}/conf/"
export HADOOP_CONF_DIR="/home/${username}/${DIST}/conf/"
export HADOOP_LOG_DIR="/home/${username}/${DIST}/tmp/"
export JAVA_HOME="/usr/lib/jvm/java-6-sun/"
export MAX_CLIENT_RETRIES="0"
#export KTHFS_INODE_CACHE=true



