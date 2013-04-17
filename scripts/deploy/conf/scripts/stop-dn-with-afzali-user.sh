#!/bin/sh

#cmd="/home/kthfs/salman/hdfs-salman/stop-dn.sh"
#exec /bin/su - kthfs -c "$cmd"

PID=`cat /tmp/hadoop-afzali-datanode.pid`
kill -9 $PID

exit $?

