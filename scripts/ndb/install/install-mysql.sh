#!/bin/bash

SCRIPTS_DIR=/var/lib/mysql-cluster/ndb/scripts
bin=`which $0`

if [ $# -lt 1 ] ; then
  echo "usage: $bin host mgmserver"
  exit 1
fi

HOSTNAME=$1
shift

ID=`ls $SCRIPTS_DIR/../ | grep mysql | tail 1 | sed -e 's/mysql_//'`
if [ "$ID" == "" ] ; then
  ID=1
else 
  ID=`expr $ID + 1`
fi

MGM_SERVER=$1
shift

./ndbinstaller.sh -ni -m $MGM_SERVER -mh $HOSTNAME -a amd64 -i remote-mysql -u root $@

exit $?
