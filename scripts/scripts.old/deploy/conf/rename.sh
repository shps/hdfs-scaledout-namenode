#!/bin/bash
username=kthfs 
workspace=dist

perl -pi -e 's/reader-cloud/'$1'/g' /home/$username/$workspace/conf/hdfs-site.xml
perl -pi -e 's/writer-cloud/'$1'/g' /home/$username/$workspace/conf/hdfs-site.xml
perl -pi -e 's/reader-cloud/'$1'/g' /home/$username/$workspace/conf/core-site.xml
perl -pi -e 's/writer-cloud/'$1'/g' /home/$username/$workspace/conf/core-site.xml

exit 0
