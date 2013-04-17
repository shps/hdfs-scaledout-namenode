#!/bin/sh
# this script only pushes the hdfs binaries to a single host

remoteReleaseDir=/home/kthfs/dist
file1=../../hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT.tar.gz
file2=../../hadoop-common-project/hadoop-common/target/hadoop-common-0.24.0-SNAPSHOT.tar.gz

parallel-rsync -az -h hosts.txt $file1  $remoteReleaseDir
parallel-rsync -az -h hosts.txt $file2  $remoteReleaseDir
