#!/bin/sh
HOSTS=`cut -f 1 -d " " ../hosts.txt`
TEST=loadgen
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
        cat $RES-DIR/$i-$TEST-results.txt | egrep '^Average operations per second:' | sed -e 's/Average operations per second://g' | sed -e 's/ops\/s//g' | awk '{print $1}' >> $RES
        HOST_ID=`echo $i | sed -e 's/cloud//g' | sed -e 's/\.sics\.se//g'`
        echo "$HOST_ID" >> $RES_DIR/hosts.txt
done
grep "operations per second" results/* | awk '{print $5}' | awk -Fops/s '{print $1}' | awk '{s+=$1} END {print s}' > $RES_DIR/summary.txt

paste $RES_DIR/hosts.txt $RES >> $RES_DIR/summary.txt
rm $RES_DIR/hosts.txt $RES_DIR/results.txt

cat $RES_DIR/summary.txt
exit 0
