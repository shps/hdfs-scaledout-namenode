#!/bin/bash

#user name. reading from file
#note all the scripts should use same user name
username=`cut -f 1 -d " " username`

#folder on the remote machine where every thing will be copied
DIST=`cut -f 1 -d " " distribution_folder_name`
distribution_folder="/home/$username/$DIST"
echo "Distribution Folder: $distribution_folder"

if [ -z $DIST ]; then
	echo "Error: unable to read file 'distribution_folder_name'"
	return
fi
if [ -z $username ]; then
	echo "Error: unable to read file 'username'"
	return
fi


export HADOOP_DEV_HOME=$distribution_folder;  			
export HADOOP_COMMON_HOME=$HADOOP_DEV_HOME;			
export HADOOP_HDFS_HOME=$HADOOP_DEV_HOME;			
export HADOOP_CONF_DIR=$HADOOP_DEV_HOME/conf/hdfs_configs/;	
export HADOOP_HOME=$HADOOP_DEV_HOME;				
export JAVA_HOME=/usr/lib/jvm/java-6-sun/;			
export LD_LIBRARY_PATH=$HADOOP_DEV_HOME/conf/ndb_lib/;		
export HADOOP_PID_DIR=/tmp/;					

echo "export HADOOP_DEV_HOME=$HADOOP_DEV_HOME"
echo "export HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME"
echo "export HADOOP_HDFS_HOME=$HADOOP_HDFS_HOME"
echo "export HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "export HADOOP_HOME=$HADOOP_HOME"
echo "export JAVA_HOME=$JAVA_HOME"
echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "export PID=$HADOOP_PID_DIR"

# Extra Java runtime options. Empty by default.
export HADOOP_OPTS='-Dcom.sun.management.jmxremote.authenticate=true -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.password.file='$HADOOP_DEV_HOME'/etc/hadoop/jmxremote.password'

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS='-Dcom.sun.management.jmxremote  -Dcom.sun.management.jmxremote.port=8077'
#export HADOOP_SECONDARYNAMENODE_OPTS='-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8099'
export HADOOP_DATANODE_OPTS='-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8078'
#export HADOOP_BALANCER_OPTS='-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=80'
#export HADOOP_JOBTRACKER_OPTS='-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8008'
#export HADOOP_TASKTRACKER_OPTS='-Dcom.sun.management.jmxremote.port=8009'

echo "JMX options are set"

