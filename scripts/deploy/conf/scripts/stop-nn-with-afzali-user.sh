#!/bin/sh

username=afzali
rootDir=/home/$username/kthfs-demo/hadoop

cmd=$rootDir/sbin/scripts/stop-nn.sh
exec /bin/su - $username -c "$cmd"

exit $?

