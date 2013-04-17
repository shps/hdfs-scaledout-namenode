#!/bin/bash
DIST=dist
username=kthfs


. ./set-env.sh


rm -rf /home/${username}/${DIST}/tmp/*
echo "Formatting the namenode..."
$HADOOP_HDFS_HOME/bin/hdfs namenode -format 2>&1 > /home/${username}/${DIST}/namenode-format.log > /dev/null

nohup $HADOOP_HDFS_HOME/bin/hdfs namenode > /home/${username}/${DIST}/namenode.log &
sleep 5
nohup $HADOOP_HDFS_HOME/bin/hdfs datanode > /home/${username}/${DIST}/datanode.log &

exit

