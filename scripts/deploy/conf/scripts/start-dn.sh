#!/bin/bash

. ./set-env.sh

username=`cut -f 1 -d " " username`
dist=`cut -f 1 -d " " distribution_folder_name`
rootDir="/home/$username/$dist"

HDFS=$HADOOP_COMMON_HOME/bin/hdfs

echo "Starting the datanode. $HDFS datanode 2> $rootDir/logs/datanode.out &"

nohup $HDFS datanode 2> $rootDir/logs/datanode.out &

exit $?
