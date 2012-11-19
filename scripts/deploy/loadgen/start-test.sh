#!/bin/sh
HOSTS=`cut -f 1 -d " " ../hosts.txt`
releaseDir=/home/kthfs/dist

if [ $# -lt 1 ] ; then
echo "Usage: prog numFiles [readPercent]"
exit 0
fi

READ_PERCENT=100 
if [ $# -eq 2 ] ; then
 READ_PERCENT=$2
fi

for i in $HOSTS
do
	connectStr="kthfs@${i}"	
	#echo "Running $connectStr $releaseDir/conf/leave-safe-mode.sh"
	ssh $connectStr "$releaseDir/conf/leave-safe-mode.sh 1,2> /dev/null" < /dev/null 
	echo "Running $connectStr $releaseDir/loadgen.sh $i $1"
	ssh $connectStr "nohup $releaseDir/loadgen.sh $i $1 $READ_PERCENT > $releaseDir/loadgen.out 2> $releaseDir/loadgen.err < /dev/null &"
	#sleep 1
done
