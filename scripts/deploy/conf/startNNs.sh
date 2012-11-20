#!/bin/sh
DIST=dist
username=kthfs

export HADOOP_HDFS_HOME="/home/${username}/${DIST}/hadoop-hdfs-0.24.0-SNAPSHOT"
export HADOOP_COMMON_HOME="/home/${username}/${DIST}/hadoop-common-0.24.0-SNAPSHOT"
export LD_LIBRARY_PATH="/home/${username}/${DIST}/conf/"
export HADOOP_LOG_DIR="/home/${username}/${DIST}/tmp/"
export JAVA_HOME="/usr/lib/jvm/java-6-sun/"

rm -rf /home/${username}/${DIST}/tmp/*
echo "Formatting the namenode..."
$HADOOP_HDFS_HOME/bin/hdfs namenode -format 2>&1 > /home/${username}/${DIST}/namenode-format.log > /dev/null
nohup $HADOOP_HDFS_HOME/bin/hdfs namenode > /home/${username}/${DIST}/namenode.log &

exit 0
