#!/bin/bash

# Author: Hooman Peiro Sajjad 
# Email: hooman@sics.se
# This script runs NNThroughputBenchmark over an instance of HDFS.
#	These are the valid operations on NNThroughputBenchmark:
#	-op blockReport [-datanodes T] [-reports N] [-blocksPerReport B] [-blocksPerFile F] | 
#  [-keepResults] | [-logLevel L] | [-UGCacheRefreshCount G]
											 
if [ ! -e /usr/bin/memcached ] ; then
echo "You do not appear to have installed: memcached"
echo "sudo apt-get install memcached"
sudo apt-get install memcached
fi


if [ -z "$1" ]
then
	echo "Error: You must select the number of iterations per experiment."
	echo "Usage: ./throughput-blockreport 1"
	echo "where 1: no. of experiments"
	exit
fi


#op=$1 # benchmark type
nrRounds=$1 # number of experiments

# blockReport
crod () {
	operation="-op blockReport -datanodes $1 -reports $2 -blocksPerReport $3 -blocksPerFile $4"
	count=$nrRounds
	
	#if [ $6 -eq 1 ]
	#then
	#	operation="$operation -useExisting"
	#fi
	operation="$operation -keepResults"

	echo "starting operation: $operation"
	for (( i=1; i<=$count; i++ ))
	do
		$HADOOP_COMMON_HOME/bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark $operation | tee $logFile
		grep -i -e "reports = *" -e "datanodes = *" -e "blocksPerReport = *" -e "blocksPerFile = *" -e "# operations:*" -e "Elapsed Time:*" -e "Ops per sec*" -e "Average Time:*" $logFile | awk '{printf "%s\t\t", $NF}END{print ""}' >> $outputFile
	 done
	nrReports=$(awk < $outputFile 'NR==1{print $1}') #Number of block report operations
	nrDatanodes=$(awk < $outputFile 'NR==1{print $2}') #Number of datanodes
	nrBlocksPerReport=$(awk < $outputFile 'NR==1{print $3}') #Number of blocks per report
	nrBlocksPerFile=$(awk < $outputFile 'NR==1{print $4}') #Number of blocks per file
	operations=$(awk < $outputFile 'NR==1{print $5}') # Number of operations
	avgETime=$(awk < $outputFile '{ sum+=$6 } END {print sum}') #Average elapsed time of each experiment
	avgETime=$((avgETime/count))
	avgOps=$(awk < $outputFile '{ sum+=$7 } END {print sum}') #Average number of operation per second
	avgOps=$(echo "scale=5;$avgOps/$count" | bc)
	avgTimeOp=$(awk < $outputFile '{ sum+=$8 } END {print sum}') #Average time of each operation
	avgTimeOp=$((avgTimeOp/count))
	echo -e "$nrReports\t\t$nrDatanodes\t\t\t$nrBlocksPerReport\t\t\t$nrBlocksPerFile\t\t\t$operations\t\t\t$avgETime\t\t\t$avgOps\t\t\t$avgTimeOp\n" >> $avgFile

	./restart-memcache
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

echo "Setting the environment variables..."
. ./set-env.sh $rootDir

echo "Starting memcache"
./restart-memcache

#echo "Formatting the namenode..."
#$HADOOP_HDFS_HOME/bin/hdfs namenode -format

outputFile="result.txt"
logFile="benchmark-log.txt"
avgFile="avg-result.txt"
echo "Starting the benchmark..."
> $logFile
echo -e "#NrReports\t\tNrDatanodes\t\t\tNrBlocksPerReport\t\t\tNrBlocksPerFile\t\t\tOperations\t\t\tAvgElapsedTime\t\t\tAvgOpsPerSec\t\t\tAverageTimeofOp" > $avgFile

#initial values
startT=1
endT=1
#no. of block report operations in total
reports=5000
# total no. of datanodes
# the total block reports would be divided among these datanodes
# the block reports assigned to these datanodes would send their blocks to the NN
datanodes=35
blocksPerReport=2000
#blocksPerFile=2
useExisting=0
# You can change the while loop to modify the trends
while [ $startT -le $endT ]
do
	> $outputFile
	crod $datanodes $reports $blocksPerReport $blocksPerFile $nrRounds $useExisting
	#datanodes=$[$datanodes + 2]
	#blocksPerReport=$[$blocksPerReport + 1000]
	#blocksPerFile=$[$blocksPerFile + 100]
	useExisting=1
	startT=$[$startT + 1]

done
echo "Benchmark finished."
