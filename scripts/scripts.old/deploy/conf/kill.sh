#!/bin/sh

echo "** Killing existing datanodes/namenodes"
for i in `ps -ef | grep kthfs | grep datanode | awk '{print $2}'`
do
kill -9 $i > /dev/null 2>&1
done

for i in `ps -ef | grep kthfs | grep namenode | awk '{print $2}'`
do
kill -9 $i > /dev/null 2>&1
done


