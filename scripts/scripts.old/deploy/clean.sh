#!/bin/bash
# Author: Jim Dowling

DIST=dist
username=kthfs
remoteReleaseDir=/home/$username/$DIST

CLEAN="rm -rf $remoteReleaseDir"
     
HOSTS=`cut -f 1 -d " " ./hosts.txt`

for i in $HOSTS
do
 echo "Cleaning $remoteReleaseDir on $i"
 connectStr="$username@$i"
 ssh $connectStr ''$CLEAN''
done

exit 0
