#!/bin/sh

username=kthfs
releaseDir=/home/$username/dist
HOSTS=`cut -f 1 -d " " ./hosts.txt`

echo ""
for i in $HOSTS
do
	connectStr="$username@$i"	
	echo "Running $releaseDir/kill.sh on $connectStr"
	ssh $connectStr "$releaseDir/kill.sh "
	sleep 5
done
exit 0
