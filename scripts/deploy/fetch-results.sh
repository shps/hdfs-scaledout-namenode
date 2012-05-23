#!/bin/sh

username=kthfs
releaseDir=/home/kthfs/release2
remoteReleaseDir=/home/kthfs/release

for i in $@
do
	connectStr="$username@$i.sics.se"	
	remotePath=$connectStr:$remoteReleaseDir
	
	scp $remotePath/loadgen.out $releaseDir/scripts/results/$i-loadgen-results.txt
done
