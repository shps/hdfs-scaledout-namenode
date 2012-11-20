#!/bin/bash
HOSTS=`cut -f 1 -d " " ./hosts.txt`
releaseDir=/root
rm -rf results/*
for i in $HOSTS
do
	connectStr="root@${i}"	
	ssh $connectStr "rm $releaseDir/flex.out"
	echo "Cleaning $connectStr flex.out"
done

