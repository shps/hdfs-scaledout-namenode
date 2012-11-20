#!/bin/bash

bin=`which $0`

if [ $# -lt 3 ] ; then
  echo "usage: $bin myHostname dataMemorySize -d host1 -d host2 [hostN ...]"
  exit 1
fi

HOST=$1
shift
DM=$1
shift

NUM=0
HOSTS=
while [ $1 == "-d" ]; do
  shift
  HOSTS="$HOSTS -d $1" 
  NUM=`expr $NUM + 1`
  shift
done

./ndbinstaller.sh -ni -m $HOST -dm $DM -n $NUM -a amd64 $HOSTS -i mgm -u root $@

exit 0
