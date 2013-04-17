#!/bin/bash

. ./set-env.sh

username=`cut -f 1 -d " " username`

echo "** Killing existing namenodes"
for i in `ps -ef | grep $username | grep namenode | awk '{print $2}'`
do
kill -9 $i > /dev/null 2>&1
done


exit $?
