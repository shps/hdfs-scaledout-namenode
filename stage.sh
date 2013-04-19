#!/bin/bash

rm -rf staging

set -e

./build.sh

mkdir -p staging/hadoop
dir1=hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT/
dir2=hadoop-common-project/hadoop-common/target/hadoop-common-0.24.0-SNAPSHOT/


cp -r $dir1/* staging/hadoop
cp -r $dir2/* staging/hadoop

echo ""
echo "Copying binaries to snurran.sics.se"
echo ""
cd staging
tar zcf kthfs-demo.tar.gz *
scp kthfs-demo.tar.gz jdowling@snurran.sics.se:/var/www/kthfs/kthfs-demo.tar.gz 

echo ""
echo "Finished"
echo ""
exit 0

