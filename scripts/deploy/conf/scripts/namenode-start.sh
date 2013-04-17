#!/bin/bash

username=kthfs
rootDir=/home/$username/hamid/hadoop
. $rootDir/bin/scripts/set-env.sh $rootDir

./hdfs namenode

PID=$!

echo "$PID" > $rootDir/namenode.pid
exit
