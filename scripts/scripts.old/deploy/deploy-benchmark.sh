#!/bin/bash
# Author: Wasif Riaz Malik, 2011, Jim Dowling, 2012
# This script broadcasts all files required for running Namenode Throughput benchmarking. 

if [ ! -e /usr/bin/parallel-rsync ] ; then
echo "You do not appear to have installed: parallel-rsync"
echo "sudo apt-get install pssh"
fi

HOSTS=`cut -f 1 -d " " ./hosts.txt`
echo "Deploying on: $HOSTS"

username=kthfs

dir1=../../hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT
dir2=../../hadoop-common-project/hadoop-common/target/hadoop-common-0.24.0-SNAPSHOT
remoteReleaseDir=/home/$username/dest
benchmarkDir=./dest

parallel-rsync -arz -h hosts.txt $dir1  $remoteReleaseDir
parallel-rsync -arz -h hosts.txt $dir2  $remoteReleaseDir


for i in $HOSTS
do
	connectStr="$username@$i"
	ssh $connectStr 'mkdir -p '$remoteReleaseDir'/tmp'

	remotePath="$username@$i:$remoteReleaseDir"

	echo "\n** Pushing the release to $i"

	# moving all the scripts to remote directory
	scp $benchmarkDir'/config.txt' $remotePath 
	scp -r $benchmarkDir'/confs' $remotePath
	scp $benchmarkDir'/libndbclient.so' $remotePath
	scp $benchmarkDir'/set-env.sh' $remotePath
	scp $benchmarkDir'/start-memcache' $remotePath
	scp $benchmarkDir'/stop-memcache' $remotePath
	scp $benchmarkDir'/restart-memcache' $remotePath
	scp $benchmarkDir'/stop-throughput-benchmark.sh' $remotePath
	scp $benchmarkDir'/throughput-blockreport.sh' $remotePath
	scp $benchmarkDir'/throughput-readwrite.sh' $remotePath
	scp $benchmarkDir'/results_throughput_READS.txt' $remotePath

done

exit 0
