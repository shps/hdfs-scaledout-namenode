#!/bin/bash

SRC="/var/lib/collectd/rrd"
TARGET1="/home/x/.netbeans/7.1.2/config/GF3_1/domain1/data"
TARGET2="/home/x/NetBeansProjects/kthfs/KTHFSDashboard/target/KTHFSDashboard/jarmon/data"

echo "Creting Symbolic Links:"
echo
#echo $SRC" ---> "$TARGET1
echo $SRC" ---> "$TARGET2
echo
#ln -s $SRC $TARGET1
ln -s $SRC $TARGET2

rm -rf /var/lib/collectd/rrd/rrd
