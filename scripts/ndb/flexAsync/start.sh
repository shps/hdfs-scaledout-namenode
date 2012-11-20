#!/bin/sh
HOSTS=`cut -f 1 -d " " ./hosts.txt`
releaseDir=/root

if [ $# -ne 3 ] ; then
echo "Usage: $0 number_threads client_interarrival_time exec_time"
exit 1
fi

#COUNT=0
LEN=`wc -l ./hosts.txt | awk '{print $1}'`
#LEN=11
echo "number of hosts: $LEN"

for i in $HOSTS
do
	connectStr="root@${i}"
        warmup_time=`expr $2 \* $LEN`
        warmup_time=`expr $warmup_time \* 1000`
        #if [ $COUNT -eq 0 ] ; then
	  ssh $connectStr "nohup $releaseDir/flexAsync.sh $1 $warmup_time $3 > $releaseDir/flex.out &" 
          sleep $2
        #else 
	#  ssh $connectStr "nohup $releaseDir/flexAsync.sh $1 `expr  > $releaseDir/flex.out &" 
        #  sleep 2
        #fi
	echo "Running $connectStr $releaseDir/flexAsync.sh $i with warmup_time $warmup_time"
        #COUNT=`expr $COUNT + 1`
        LEN=`expr $LEN - 1`
done
