#!/bin/bash
# Author: Wasif Riaz Malik, 2011, Jim Dowling, 2012, Salman Niazi 2013
# This script broadcasts all files required for running a KTHFS instance.
# It kills already running daemons on the target nodes, but it does not start new ones. 
# If you want to remotely start kthfs daemons, see start-kthfs.sh
# The nodes are supplied as command line arguments to this script. 
# A password-less sign-on should be setup prior to calling this script

if [ ! -e /usr/bin/parallel-rsync ] ; then
echo "You do not appear to have installed: parallel-rsync"
echo "sudo aptitude install pssh"
exit
fi


#clean the project and then package it
mvn clean -f ./../../pom.xml  package -Pdist -DskipTests


# get all the hosts where hadoop will be installed
DEFAULT_NN_HOST=`cut -f 1 -d " " ./conf/scripts/default_nn.txt`
ALL_NN_HOSTS=`cut -f 1 -d " " ./conf/scripts/hosts_nn.txt`
ALL_DN_HOSTS=`cut -f 1 -d " " ./conf/scripts/hosts_dn.txt`
ALL_HOSTS_FILE_NAME="./conf/scripts/auto_gen_all_hosts.txt"
grep -h "cloud" ./conf/scripts/host*.txt > $ALL_HOSTS_FILE_NAME
ALL_HOSTS=`cut -f 1 -d " " ./conf/scripts/auto_gen_all_hosts.txt`
echo "Deploying on NN: " echo "$ALL_NN_HOSTS"
echo "Deploying on DN: " echo "$ALL_DN_HOSTS"

#user name. reading from file
#note all the scripts should use same user name
username=`cut -f 1 -d " " ./conf/scripts/username`
echo "User Name : $username"


#folder on the remote machine where every thing will be copied
DIST=`cut -f 1 -d " " ./conf/scripts/distribution_folder_name`
echo "Distribution Folder: /home/$username/$DIST"


#remoteReleaseDir=/tmp/$DIST
remoteReleaseDir=/home/$username/$DIST

for i in $ALL_HOSTS
do
 connectStr="$username@$i"
 ssh $connectStr 'mkdir -p '$remoteReleaseDir'/tmp'
done



dir1=../../hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT/
dir2=../../hadoop-common-project/hadoop-common/target/hadoop-common-0.24.0-SNAPSHOT/

parallel-rsync -arz -h $ALL_HOSTS_FILE_NAME --user $username $dir1  $remoteReleaseDir
parallel-rsync -arz -h $ALL_HOSTS_FILE_NAME --user $username $dir2  $remoteReleaseDir
parallel-rsync -arz -h $ALL_HOSTS_FILE_NAME --user $username conf  $remoteReleaseDir



for i in $ALL_NN_HOSTS
do
	echo "____ Fixing address in name node configs $i ____" 
	echo ""
	remotePath="$username@$i:$remoteReleaseDir"
	connectStr="$username@$i"

	# running some scripts and commands on the server
	ssh $connectStr 'chmod +x '$remoteReleaseDir'/conf/scripts/*.sh'
	ssh $connectStr ''$remoteReleaseDir'/conf/scripts/rename.sh '$username' '$DIST' 'core-site.xml' '$i' '
	ssh $connectStr ''$remoteReleaseDir'/conf/scripts/rename.sh '$username' '$DIST' 'hdfs-site.xml' '$i' '
done

for i in $ALL_DN_HOSTS
do
	echo "____ Fixing address in data node configs $i ____" 
	echo ""

	remotePath="$username@$i:$remoteReleaseDir"
	connectStr="$username@$i"

	# running some scripts and commands on the server
	ssh $connectStr 'chmod +x '$remoteReleaseDir'/conf/scripts/*.sh'
	ssh $connectStr ''$remoteReleaseDir'/conf/scripts/rename.sh '$username' '$DIST' 'core-site.xml' '$DEFAULT_NN_HOST' '
	ssh $connectStr ''$remoteReleaseDir'/conf/scripts/rename.sh '$username' '$DIST' 'hdfs-site.xml' '$i' '
done




for i in $ALL_HOSTS
do
	remotePath="$username@$i:$remoteReleaseDir"
	connectStr="$username@$i"

	echo "____ Moving Config files $i ____" 
	echo ""

	#moving the core-site.xml and hdfs-site.xml to etc/hadoop/
	#not using these config files. we are setting HADOOP_CONF_DIR to./conf for easy manipulation
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/hdfs_configs/core-site.xml '$remoteReleaseDir'/etc/hadoop/'
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/hdfs_configs/hdfs-site.xml '$remoteReleaseDir'/etc/hadoop/'
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/hdfs_configs/log4j.properties '$remoteReleaseDir'/etc/hadoop/'

	#copying the script files
	ssh $connectStr 'cp -r '$remoteReleaseDir'/conf/scripts '$remoteReleaseDir'/sbin/'

	#copying the JMX file
	ssh $connectStr 'cp '$remoteReleaseDir'/conf/jxm/jmxremote.password '$remoteReleaseDir'/etc/hadoop/'
	ssh $connectStr 'chmod 600 '$remoteReleaseDir'/etc/hadoop/jmxremote.password'

	#Killing existing namenodes/datanodes
	ssh $connectStr ''$remoteReleaseDir'/conf/scripts/kill.sh '$username''

	#removing the useless folders in conf folder.
	#it only confuses people
	ssh $connectStr 'rm -rf '$remoteReleaseDir'/conf/scripts'
	ssh $connectStr 'rm -rf '$remoteReleaseDir'/conf/jxm'

	#make foler for lgo
	ssh $connectStr 'mkdir '$remoteReleaseDir'/logs'
	
done




exit 0
