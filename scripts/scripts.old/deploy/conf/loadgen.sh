#!/bin/sh

if [ $# -eq 0 ] ; then
  echo "usage: prog hostname [NUM_FILES [READ_PERCENT]]"
  exit 0
fi
# Hostname of the namenode you would like to run the LoadGenerator on
NN=$1
READ_PERCENT=100
NUM_FILES=10
if [ $# -gt 1 ] ; then
 NUM_FILES=$2 
fi
if [ $# -gt 2 ] ; then
  READ_PERCENT=$3
fi
WRITE_PERCENT=`expr 100 - $READ_PERCENT`
DIST=dist
username=kthfs

export HADOOP_HDFS_HOME="/home/$username/$DIST/hadoop-hdfs-0.24.0-SNAPSHOT"
export HADOOP_COMMON_HOME="/home/$username/$DIST/hadoop-common-0.24.0-SNAPSHOT"
export LD_LIBRARY_PATH="/home/$username/$DIST/conf"
#export HADOOP_CONF_DIR="/home/$username/$DIST/conf/$NN/"
export JAVA_HOME="/usr/lib/jvm/java-6-sun/"

HADOOP=$HADOOP_COMMON_HOME/bin/hadoop
$HADOOP_HDFS_HOME/bin/hdfs dfs -rm -r /$NN

if [ ! -e /tmp/$NN ]
then
 mkdir /tmp/$NN
fi

echo "\n\n** About to run org.apache.hadoop.fs.loadGenerator.StructureGenerator"
$HADOOP org.apache.hadoop.fs.loadGenerator.StructureGenerator -outDir /tmp/$NN/ -numOfFiles $NUM_FILES -maxDepth 5 -minWidth 1 -maxWidth 5 -avgFileSize 0.0001
 
echo "\n\n** About to run org.apache.hadoop.fs.loadGenerator.DataGenerator"
$HADOOP org.apache.hadoop.fs.loadGenerator.DataGenerator -inDir /tmp/$NN/ -root /$NN

echo "\n\n** About to run org.apache.hadoop.fs.loadGenerator.LoadGenerator with readPercent $READ_PERCENT and writePercent $WRITE_PERCENT"
$HADOOP org.apache.hadoop.fs.loadGenerator.LoadGenerator -numOfThreads 12 -elapsedTime 10 -startTime 1 -root /$NN -readProbability 1.0 -writeProbability 0.0

echo "Namenode hostname: $NN"
echo "LoadGenerator hostname: `hostname`"
date
