#!/bin/bash
DIST=dist
username=kthfs

export HADOOP_HDFS_HOME="/home/$username/$DIST/hadoop-hdfs-0.24.0-SNAPSHOT"
export HADOOP_COMMON_HOME="/home/$username/$DIST/hadoop-common-0.24.0-SNAPSHOT"
export LD_LIBRARY_PATH="/home/$username/$DIST/conf"
export JAVA_HOME="/usr/lib/jvm/java-6-sun/"

$HADOOP_COMMON_HOME/bin/hadoop dfsadmin -safemode leave
