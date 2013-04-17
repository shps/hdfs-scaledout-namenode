#!/bin/bash

username=$1

workspace=$2

filename=$3

perl -pi -e 's/reader-cloud/'$4'/g' /home/$username/$workspace/conf/hdfs_configs/$filename
perl -pi -e 's/writer-cloud/'$4'/g' /home/$username/$workspace/conf/hdfs_configs/$filename


exit 0
