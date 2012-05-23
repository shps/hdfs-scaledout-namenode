#!/bin/sh

username=kthfs
releaseDir=/home/kthfs/release

echo ""
for i in $@
do
	connectStr="$username@$i.sics.se"	
	echo "Running $releaseDir/startProcs.sh on $connectStr"
	ssh $connectStr "nohup $releaseDir/startProcs.sh > $releaseDir/procs.out 2> $releaseDir/procs.err < /dev/null &"
	sleep 5
done

echo "\n## The instances might take a few seconds to become active\n"
