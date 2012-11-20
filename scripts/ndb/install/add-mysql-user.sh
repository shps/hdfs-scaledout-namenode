#!/bin/bash

SCRIPTS_DIR=/var/lib/mysql-cluster/ndb/scripts
bin=`which $0`

if [ $# -lt 1 ] ; then
  echo "usage: $bin username password"
  exit 1
fi

ID=`ls $SCRIPTS_DIR/../ | grep mysql | sed -e 's/mysql_//'`
if [ "$ID" == "" ] ; then
  echo "No mysql server found on this host."
  exit 1
fi

USERNAME=$1
shift
PASSWORD=$1
shift

for i in $ID ; do
    $SCRIPTS_DIR/mysql-client-$ID.sh -S -e "GRANT ALL PRIVILEGES ON *.* to '$USERNAME'@'%' IDENTIFIED BY '$PASSWORD'"
if [ $? -ne 0 ] ; then
  echo "Problem add user to mysql server $i"
  exit 1
fi
done

exit $?
