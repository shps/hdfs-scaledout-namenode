#!/bin/bash
username=kthfs 
DIST=dist

perl -pi -e 's/reader-cloud/'$1'/g' /home/$username/$DIST/conf/hdfs-site.xml
perl -pi -e 's/writer-cloud/'$1'/g' /home/$username/$DIST/conf/hdfs-site.xml
perl -pi -e 's/reader-cloud/'$1'/g' /home/$username/$DIST/conf/core-site.xml
perl -pi -e 's/writer-cloud/'$1'/g' /home/$username/$DIST/conf/core-site.xml

exit 0
