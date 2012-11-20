#!/bin/sh
HOSTS=`cut -f 1 -d " " ./hosts.txt`
releaseDir=/root
mkdir results 2> /dev/null
rm -rf results/*
for i in $HOSTS
do
	connectStr="root@${i}:$releaseDir/flex.out"	
	scp $connectStr "results/$i.out"
	echo "Downloading $connectStr $i"
done
