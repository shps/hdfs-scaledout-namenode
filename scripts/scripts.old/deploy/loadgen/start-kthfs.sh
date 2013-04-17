#!/bin/sh

username=kthfs
releaseDir=/home/$username/dist
HOSTS=`cut -f 1 -d " " ../hosts.txt`

echo ""
for i in $HOSTS
do
	connectStr="$username@$i"	
	echo "Running $releaseDir/startProcs.sh on $connectStr"
	ssh $connectStr "nohup $releaseDir/startProcs.sh > $releaseDir/procs.out 2> $releaseDir/procs.err < /dev/null &"
	sleep 5
done

echo "\n## The instances might take a few seconds to become active\n"
