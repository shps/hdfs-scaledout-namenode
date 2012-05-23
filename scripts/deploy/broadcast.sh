#!/bin/sh
# Author: Wasif Riaz Malik, 2011
# This script broadcasts all files required for running a KTHFS instance. It kills already running daemons on the target hostnames, but it does not start new ones. If you want to remotely start kthfs daemons, use start-kthfs.sh
# The hostnames are supplied as command line arguments to this script. A password-less sign-on should be setup prior to calling this script
#
# The following conf files for each host should be present in the $releaseDir/conf/hostname/ directory
#      hdfs-site.xml, core-site.xml, startProcs.sh, kill.sh, loadgen.sh
username=kthfs
releaseDir=/home/kthfs/release2/
remoteReleaseDir=/home/kthfs/release
file1=$HADOOP_HDFS_HOME/../hadoop-hdfs-0.24.0-SNAPSHOT.tar.gz
file2=$HADOOP_COMMON_HOME/../hadoop-common-0.24.0-SNAPSHOT.tar.gz
file3=$releaseDir/scripts/untar.sh
file7=$releaseDir/conf/libndbclient.so
file8=$releaseDir/conf/log4j.properties

for i in $@
do
	remotePath="$username@$i.sics.se:$remoteReleaseDir"
	connectStr="$username@$i.sics.se"

	echo "\n** Pushing the release to $i"
	scp $file1 $remotePath
	scp $file2 $remotePath
	scp $file3 $remotePath
	scp $file7 $remotePath
	scp $file8 $remotePath
	scp $releaseDir/conf/$i/hdfs-site.xml $remotePath
	scp $releaseDir/conf/$i/core-site.xml $remotePath
	scp $releaseDir/conf/$i/startProcs.sh $remotePath
	scp $releaseDir/conf/$i/loadgen.sh $remotePath
	scp $releaseDir/conf/$i/kill.sh $remotePath

	
	echo "[Unpacking and configuring the release]"
	ssh $connectStr $remoteReleaseDir'/untar.sh' 2>&1 >> /dev/null

	# moving the config files to common/etc/hadoop
	ssh $connectStr 'mv '$remoteReleaseDir'/*xml '$remoteReleaseDir'/hadoop-common*/etc/hadoop/'
	ssh $connectStr 'cp '$remoteReleaseDir'/log4j.properties '$remoteReleaseDir'/hadoop-common*/etc/hadoop/'

	# tarballs not required anymore - removing
	ssh $connectStr 'rm -rf '$remoteReleaseDir'/*.tar.gz'
	ssh $connectStr 'rm -rf '$remoteReleaseDir'/untar.sh'

	#creating a tmp dir for namenode and datanode
	ssh $connectStr 'mkdir '$remoteReleaseDir'/tmp' >> /dev/null 2>&1

	#Killing existing namenodes/datanodes
	ssh $connectStr "$remoteReleaseDir/kill.sh"

done

