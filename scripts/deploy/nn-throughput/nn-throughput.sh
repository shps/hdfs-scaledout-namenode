#!/bin/sh
HOSTS=`cut -f 1 -d " " ../hosts.txt`
USER=kthfs
DIST=dist
LOG=nnthru
releaseDir=/home/$USER/$DIST

if [ $# -lt 1 ] ; then
echo "Usage: prog [params]"
exit 0
fi

for i in $HOSTS
do
 connectStr="$USER@${i}"	
 echo "Running $connectStr $releaseDir/conf/throughput-benchmark.sh $i $@"
 ssh $connectStr "nohup $releaseDir/conf/throughput-benchmark.sh $i $@ > $releaseDir/$LOG.out 2> $releaseDir/$LOG.err < /dev/null &"
	#sleep 1
done
