##########################################################################
# get the source code
git clone ssh://kthfs@dight.sics.se/var/git/personal/kthfs/kthfs.git 
cd kthfs

# checkout the stable branch
git checkout thesis-HA

# build/package the source code into a distributable binary
mvn clean install package -Dtar -Pdist -P-cbuild -DskipTests

# setup some environment variables
export HADOOP_HDFS_HOME=`pwd`/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT
export HADOOP_COMMON_HOME=`pwd`/hadoop-common-project/hadoop-common/target/hadoop-common-0.24.0-SNAPSHOT/

# format the namenode (this creates dirs/files for the namenode on local disk)
$HADOOP_HDFS_HOME/bin/hdfs namenode -format

# start the namenode (starts up a writer namenode by default)
$HADOOP_HDFS_HOME/bin/hdfs namenode

# start a datanode
$HADOOP_HDFS_HOME/bin/hdfs datanode

# store a file in HDFS
bin/hdfs dfs -copyFromLocal /path/to/some/local/file /remotefile

# NOTE: all configuration files are present in $HADOOP_COMMON_HOME/etc/hadoop
##########################################################################
