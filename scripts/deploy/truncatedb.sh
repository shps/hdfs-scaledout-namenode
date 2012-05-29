#!/bin/sh
# Author: Wasif Riaz Malik
# Date: Jan 26, 2012

# Script to execute MySQL Routines remotely
# Usage: ./truncate.sh username password database

if [ $# -ne 3 ]; then
 echo ""
 echo "Usage: ./truncatedb.sh username password database"
 echo ""
 exit
fi

HOST=cloud11.sics.se
PORT=3307
USERNAME=$1
PASSWORD=$2
DATABASE="$3"
STATEMENT="call trunc_kthfs"

echo "mysql $DATABASE -h $HOST -P $PORT -u$USERNAME -p$PASSWORD -e $STATEMENT"
mysql $DATABASE -h $HOST -P $PORT -u$USERNAME -p$PASSWORD -e "$STATEMENT"
