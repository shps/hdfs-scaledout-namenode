#!/bin/sh
username=afzali
rootDir=/home/$username/kthfs-demo/hadoop
cmd=$rootDir/sbin/scripts/start-dn.sh

exec /bin/su - $username -c "$cmd"

exit $?

