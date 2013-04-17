#!/bin/bash

. ./set-env.sh

username=`cut -f 1 -d " " username`
dist=`cut -f 1 -d " " distribution_folder_name`
rootDir="/home/$username/$dist"

HDFS=$HADOOP_COMMON_HOME/bin/hdfs


$HDFS namenode -format


exit
