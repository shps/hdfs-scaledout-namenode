#!/bin/sh
HOSTS=`cut -f 1 -d " " ../hosts.txt`
TEST=nnthru
DIST=dist
USER=kthfs
releaseDir=/home/$USER/$DIST
remoteReleaseDir=/home/$USER/$DIST
RES_DIR=$TEST-results
mkdir -p $RES_DIR
rm -rf $RES_DIR/* 
RES=$RES_DIR/results.txt
touch $RES
for i in $HOSTS
do
	connectStr="$USER@${i}"	
	remotePath=$connectStr:$remoteReleaseDir
	scp $remotePath/$TEST.out results/$i-$TEST-results.txt
        HOST_ID=`echo $i | sed -e 's/cloud//g' | sed -e 's/\.sics\.se//g'`
        echo "$HOST_ID" >> $RES_DIR/hosts.txt
done
exit 0
