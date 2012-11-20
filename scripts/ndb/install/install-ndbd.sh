#!/bin/bash

bin=`which $0`

if [ $# -lt 2 ] ; then
  echo "usage: $bin mgm-server id [params from ndbinstaller.sh]"
  exit 1
fi

HOST=$1
shift
ID=$1
shift

./ndbinstaller.sh -ni -m $HOST -a amd64 -i ndbd -id $ID -u root -sk $@

exit 0
