#!/bin/sh

username=kthfs
releaseDir=/home/kthfs/release

for i in $@
do
	connectStr="$username@$i.sics.se"	
	echo "Running $connectStr $releaseDir/loadgen.sh $i"
	ssh $connectStr "nohup $releaseDir/loadgen.sh $i > $releaseDir/loadgen.out 2> $releaseDir/loadgen.err < /dev/null &"
	#sleep 1
done
