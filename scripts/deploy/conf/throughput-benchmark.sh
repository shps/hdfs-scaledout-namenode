#!/bin/bash

# Author: Hooman Peiro Sajjad, Jim Dowling
# Email: hooman@sics.se
# This script runs NNThroughputBenchmark over an instance of HDFS.
#	These are the valid operations on NNThroughputBenchmark:
#	-op all <other ops options> | 
#	-op create [-threads T] [-files N] [-filesPerDir P] [-close] | 
#	-op open [-threads T] [-files N] [-filesPerDir P] [-useExisting] | 
#	-op delete [-threads T] [-files N] [-filesPerDir P] [-useExisting] | 
#	-op fileStatus [-threads T] [-files N] [-filesPerDir P] [-useExisting] | 
#	-op rename [-threads T] [-files N] [-filesPerDir P] [-useExisting] | 
#	-op blockReport [-datanodes T] [-reports N] [-blocksPerReport B] [-blocksPerFile F] | 
#	-op replication [-datanodes T] [-nodesToDecommission D] [-nodeReplicationLimit C] [-totalBlocks B] [-replication R] | 
#	-op clean | 
#  [-keepResults] | [-logLevel L] | [-UGCacheRefreshCount G]
	
if [ $# -ne 2 ] ; then
 echo "usage: $0 operationType numIterations"
 echo "       valid ops - "
 exit 1
fi

export HADOOP_HDFS_HOME="../hadoop-hdfs-0.24.0-SNAPSHOT/"
export HADOOP_COMMMON_HOME="../hadoop-common-0.24.0-SNAPSHOT/"
export KTHFS_DATABASE=jude
export KTHFS_CONN_STR=cloud11.sics.se
export HADOOP_DEV_HOME="."
export KTHFS_DATABASE=jude
export LD_LIBRARY_PATH=$HADOOP_DEV_HOME/

op=$1 # benchmark type
nrRounds=$2 # number of experiments

# create - rename - update - delete - open - fileStatus
crod () {
	operation="-nn $1 -op $2 -threads $3 -files $4 -filesPerDir $5"
	count=$6
	if [ $2 != "create" ]
	then
		operation="$operation -keepResults"
		if [ $7 -eq 1 ]
		then
			operation="$operation -useExisting"
		fi
	fi
	echo "starting operation: $operation"
	for (( i=1; i<=$count; i++ ))
	do
		#$HADOOP_COMMON_HOME/bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark $operation | tee $logFile
		$HADOOP_COMMON_HOME/bin/hadoop org.apache.hadoop.hdfs.server.namenode.MultiNNThroughputBenchmark $operation | tee $logFile
		grep -i -e "nrNN = *" -e "nrFiles = *" -e "nrThreads = *" -e "nrFilesPerDir = *" -e "# operations:*" -e "Elapsed Time:*" -e "Ops per sec*" -e "Average Time:*" $logFile | awk '{printf "%s\t\t", $NF}END{print ""}' >> $outputFile
	 done
        nrNN=$(awk < $outputFile 'NR==1{print $1}') #Number of namenodes
	nrFiles=$(awk < $outputFile 'NR==1{print $2}') #Number of files
	nrThreads=$(awk < $outputFile 'NR==1{print $3}') #Number of threads
	nrFilesPerDir=$(awk < $outputFile 'NR==1{print $4}') #Number of files per directory
	operations=$(awk < $outputFile 'NR==1{print $5}') # Number of operations
	avgETime=$(awk < $outputFile '{ sum+=$6 } END {print sum}') #Average elapsed time of each experiment
	avgETime=$((avgETime/count))
	avgOps=$(awk < $outputFile '{ sum+=$7 } END {print sum}') #Average number of operation per second
	avgOps=$(echo "scale=5;$avgOps/$count" | bc)
	avgTimeOp=$(awk < $outputFile '{ sum+=$8 } END {print sum}') #Average time of each operation
	avgTimeOp=$((avgTimeOp/count))
	echo -e "$nrNN\t\t$nrFiles\t\t$nrThreads\t\t$nrFilesPerDir\t\t$operations\t\t$avgETime\t\t$avgOps\t$avgTimeOp\n" >> $avgFile
}

configs=`cut -d"=" -f 1,2 ./config.txt`
for c in $configs
do
	key=${c%=*}
	value=${c#*=}
	if [ $key == "home" ]; then
		rootDir=$value
	else
		echo "Error: No home direcotory is specified."
		exit
	fi
done

echo "root directory = $rootDir"

echo "Starting memcache"
./stop-memcache.sh
./start-memcache.sh


#echo "Formatting the namenode..."
#$HADOOP_HDFS_HOME/bin/hdfs namenode -format

outputFile="result.txt"
logFile="benchmark-log.txt"
avgFile="avg-result.txt"
echo "Starting the benchmark..."
> $logFile
echo -e "#NrNN\tNrFiles\tNrThreads\tNrFilesPerDir\tOperations\tAvgElapsedTime\tAvgOpsPerSec\tAverageTimeofOp" > $avgFile

#initial values
nn=1
threads=10
maxNN=15
files=1000000
filesPerDir=500000
useExisting=0
# You can change the while loop to modify the trends
while [ $nn -le $maxNN ]
do
	> $outputFile
	crod $nn $op $threads $files $filesPerDir $nrRounds $useExisting
	useExisting=1
	nn=$[$nn + 2]
done
echo "Benchmark finished."