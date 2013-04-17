#!/bin/sh

# Hostname of the namenode you would like to run the LoadGenerator on
NN=$1
NUM_THREADS=$2
export HADOOP_HDFS_HOME='/home/kthfs/release/hadoop-hdfs-0.24.0-SNAPSHOT'
export HADOOP_COMMON_HOME='/home/kthfs/release/hadoop-common-0.24.0-SNAPSHOT'
export LD_LIBRARY_PATH='/home/kthfs/release/'
export JAVA_HOME='/usr/lib/jvm/java-6-sun/'

HADOOP=$HADOOP_COMMON_HOME/bin/hadoop
$HADOOP_HDFS_HOME/bin/hdfs dfs -rm -r /$NN

if [ ! -e /tmp/$NN ]
then
	mkdir /tmp/$NN
fi

echo "\n\n** About to run org.apache.hadoop.fs.loadGenerator.StructureGenerator"
#$HADOOP org.apache.hadoop.fs.loadGenerator.StructureGenerator -outDir /tmp/$NN/  
$HADOOP org.apache.hadoop.fs.loadGenerator.StructureGenerator -maxDepth 1 -minWidth 1 -maxWidth 5 -numOfFiles 50 -outDir /tmp/$NN/  
 
echo "\n\n** About to run org.apache.hadoop.fs.loadGenerator.DataGenerator"
$HADOOP org.apache.hadoop.fs.loadGenerator.DataGenerator -inDir /tmp/$NN/ -root /$NN

echo "\n\n** About to run org.apache.hadoop.fs.loadGenerator.LoadGenerator"
$HADOOP org.apache.hadoop.fs.loadGenerator.LoadGenerator -numOfThreads 50 -elapsedTime 20 -startTime 1 -root /$NN -readProbability 1.0 -writeProbability 0.0

echo "Namenode hostname: $NN"
echo "LoadGenerator hostname: `hostname`"
date
