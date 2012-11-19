#!/bin/bash
# Author: Wasif Riaz Malik, 2011, Jim Dowling, 2012
# This script broadcasts all files required for running a KTHFS instance. It kills already running daemons on the target nodes, but it does not start new ones. If you want to remotely start kthfs daemons, see start-kthfs.sh
# The nodes are supplied as command line arguments to this script. A password-less sign-on should be setup prior to calling this script

if [ ! -e /usr/bin/parallel-rsync ] ; then
echo "You do not appear to have installed: parallel-rsync"
echo "sudo aptitude install pssh"
fi

HOSTS=`cut -f 1 -d " " ./hosts.txt`
echo "Deploying on: $HOSTS"

DIST=dist
username=kthfs
remoteReleaseDir=/home/$username/$DIST

for i in $HOSTS
do
 connectStr="$username@$i"
 ssh $connectStr 'mkdir -p '$remoteReleaseDir'/tmp'
done

dir1=../../hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT
dir2=../../hadoop-common-project/hadoop-common/target/hadoop-common-0.24.0-SNAPSHOT

parallel-rsync -arz -h hosts.txt $dir1  $remoteReleaseDir
parallel-rsync -arz -h hosts.txt $dir2  $remoteReleaseDir
parallel-rsync -arz -h hosts.txt conf  $remoteReleaseDir

for i in $HOSTS
do
	remotePath="$username@$i:$remoteReleaseDir"
	connectStr="$username@$i"

	echo "\n** Pushing the release to $i"

	# moving the config files to common/etc/hadoop
	ssh $connectStr ''$remoteReleaseDir'/conf/rename.sh '$i' '
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/*xml '$remoteReleaseDir'/hadoop-common*/etc/hadoop/'
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/hdfs-site.xml '$remoteReleaseDir'/hadoop-hdfs*/etc/hadoop/'
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/log4j.properties '$remoteReleaseDir'/hadoop-common*/etc/hadoop/'
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/startProcs.sh '$remoteReleaseDir'/'
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/loadgen.sh '$remoteReleaseDir'/'
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/kill.sh '$remoteReleaseDir'/'

	#Killing existing namenodes/datanodes
	ssh $connectStr ''$remoteReleaseDir'/kill.sh'
done
exit 0
