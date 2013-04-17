#!/bin/bash
ls
. ./set-env.sh

if [ "$1" == "-format" ]; then
        echo "Formatting the Namenode";
	$HADOOP_HDFS_HOME/bin/hdfs namenode -format
        
else
        echo "normal";
	$HADOOP_HDFS_HOME/bin/hdfs namenode
fi


