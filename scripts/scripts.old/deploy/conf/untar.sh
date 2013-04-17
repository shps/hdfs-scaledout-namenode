#!/bin/sh
DIST=dist
username=kthfs
for file in `ls /home/$username/$DIST/*.gz`
do
tar -xzvf $file -C /home/$username/$DIST
done
