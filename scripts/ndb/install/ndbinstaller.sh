#!/bin/bash 
#-xv

###################################################################################################
#                                                                                                 #
# This code is released under the GNU General Public License, Version 3, see for details:         #
# http://www.gnu.org/licenses/gpl-3.0.txt                                                         #
#                                                                                                 #
# Commercial licenses can be acquired by contacting info@www.jimdowling.info                      #
#                                                                                                 #
# Copyright (c) Jim Dowling, 2007,2009.                                                           #
# All Rights Reserved.                                                                            #
#                                                                                                 #
###################################################################################################

###################################################################################################
#                                                                                                 #
#      ####### #      #     # ####### ######## ###### ######                                      #
#      #       #      #     # #          #     #      #    #                                      #
#      #       #      #     # #######    #     #####  #####                                       #
#      #       #      #     #       #    #     #      # #                                         #
#      ####### ###### ####### #######    #     ###### #   #                                       #
#                                                                                                 #     
#      # #   # ####### ######## ###### #      #      ###### ######                                #
#      # ##  # #          #     #    # #      #      #      #    #                                #
#      # # # # #######    #     ###### #      #      #####  #####                                 #
#      # #  ##       #    #     #    # #      #      #      # #                                   # 
#      # #   # #######    #     #    # ###### ###### ###### #   #                                 #
#                                                                                                 #
###################################################################################################



###################################################################################################
# NDB VERSION AND MIRROR CONFIG OPTIONS
###################################################################################################

NDB_INSTALLER_VERSION="Ndb Installer Version 0.2 \n\nCopyright (c) Jim Dowling, 2007/8."

PRODUCT="MySQL Cluster"
PLATFORM="linux2.6"
CPU=
RELEASE=""
#RELEASE="-telco" 
#RELEASE="-clusterj-174-alpha"

MYSQL_VERSION_MAJOR="7"
MYSQL_VERSION_MINOR="2"
MYSQL_VERSION_REV="8"

NDB_VERSION_MAJOR="7"
NDB_VERSION_MINOR="2"
NDB_VERSION_REV="8"

VERSION_PREFIX="mysql-cluster-gpl-"
#VERSION_PREFIX=""

# Where to download mysql binaries from
#MIRROR="ftp://ftp.mysql.com/pub/mysql/download/"
MIRROR="http://dev.mysql.com/get/Downloads/MySQL-Cluster-"$MYSQL_VERSION_MAJOR"."$MYSQL_VERSION_MINOR"/mysql-cluster-gpl-"
MIRROR_DIR="/from/http://ftp.sunet.se/pub/unix/databases/relational/mysql/"
#MIRROR_DIR="cluster_telco"


#
# source:
# http://dev.mysql.com/get/Downloads/MySQL-Cluster-7.2/mysql-cluster-gpl-7.2.5-linux2.6-x86_64.tar.gz/from/http://ftp.sunet.se/pub/unix/databases/relational/mysql/
#
# E.g., VERSION="mysql-5.1.22-beta-linux-i686"; used for myql directory name
# Version numbers are constructed to look like (eg) 'mysql-5.1.24-ndb-6.2.15-telco.tar.gz'
#VERSION="mysql-${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}
#         ${NDB_VERSION_MAJOR}.${NDB_VERSION_MINOR}.${NDB_VERSION_REV}-${RELEASE}
VERSION=
# NDB_VERSION used for ndb directory name
#NDB_VERSION="-ndb-${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}${RELEASE}"
NDB_VERSION=


###################################################################################################
# Default Install Directories. Note these are changed in check_usesrid() 
# if the user does not run the script as root.
###################################################################################################
NDB_INSTALL_DIR=/var/lib/mysql-cluster
NDB_DIR=/var/lib/mysql-cluster
MYSQL_INSTALL_DIR=
#/usr/local
MYSQL_BASE_DIR=
NDB_DATADIR=ndb_data
MGM_DATADIR=mgmd1
MYSQL_BINARIES_DIR=
NDB_LOGS_DIR=
MYSQL_NUM=mysql_1
NDB_DATA_DIR=
NDBD_BIN=
NDBD_PROG=ndbmtd
#NDBD_PROG=ndbd

###################################################################################################
# NDB Data Node hostnames
###################################################################################################
# declare an array of nodes
# Max Number of Hosts set to 16. You can add more entries here.
# TODO: change this from bash (arrays) to no arrays (csh)
#declare -a NDB_HOST
#NDB_HOST[0]="localhost"
#NDB_HOST[1]="localhost"
#NDB_HOST[2]="localhost"
#NDB_HOST[3]="localhost"

NDB_HOST=('localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	  'localhost'
	)
if [ $? -ne 0 ] ; then
  exit_error "Arrays not supported in your shell version"
fi


########################################################
# NDB Default Configuration                            #
########################################################

# The 'M' is appended to DataMemory and IndexMemory sizes later in script
DATA_MEMORY=80
INDEX_MEMORY=

# Extra config.ini default values
STRING_MEMORY=25
MAX_NO_OF_TABLES=4096
MAX_NO_OF_ORDERED_INDEXES=2048
MAX_NO_OF_UNIQUE_HASH_INDEXES=4096
MAX_NO_OF_ATTRIBUTES=65536
MAX_NO_OF_CONCURRENT_SCANS=256


# Redo Log file Default Size
NUM_FRAGMENT_LOGFILES=6
FRAGMENT_LOGFILE_SIZE=64M

# Default Number of Replicas
NUM_REPLICAS=2

# Default hostname for Mgm Server
MGM_HOST=$LOCALHOST

MYSQL_HOST=$LOCALHOST

# Disk write b/w used for checkpoints
DISK_CHECKPOINT_SPEED=1

# Disk write b/w for checkpoints when restarting a node
DISK_CHECKPOINT_SPEED_IN_RESTART=10

# Disk Buffer used when writing checkpoints
DISK_SYNC_SIZE=4

# Size of the Redo Log Buffer
REDO_BUFFER=32

# This parameter states the maximum time that is permitted to lapse 
# between operations in the same transaction before the transaction is aborted.
TRANSACTION_INACTIVE_TIMEOUT=10000

# Time for master to ping all ndbds
TIME_BETWEEN_GLOBAL_CHECKPOINTS=1000

# Amount of Bytes in 2^N before local checkpoint is started
# 20 is the default value, where 4MB of writes need to be executed before a 
# Local Checkpoint (LCP) is performed. If you set to '21', it's 8MB of writes, etc.
TIME_BETWEEN_LOCAL_CHECKPOINTS=20

# Time between replication events
TIME_BETWEEN_EPOCHS=100

# Memory Usage Reports Entered into the cluster log every X/2 minutes
MEMORY_REPORT_FREQUENCY=10

# Backup reports entered into cluster log every X/2 minutes
BACKUP_REPORT_FREQUENCY=5

# Prevents cluster db from being swapped to disk
LOCK_PAGES_IN_MAIN_MEMORY=1

# Name of clusterlog file
CLUSTER_LOG_FILENAME=cluster.log

# Node-ID for the Management Server
MGMD_ID=63

########################################################
# MySQL Server Default Configuration                   #
########################################################

# Default values for my.cnf configuration variables
MYSQL_PID=
DEFAULT_MGM_PORT=1186
MGM_PORT=$DEFAULT_MGM_PORT
MYSQL_PORT=3306
MYSQL_SOCK=/tmp/mysql.sock
SKIP_INNODB="skip-innodb" 
MYSQL_HOST="127.0.0.1"  #localhost
REPLICATION_MASTER=
BINARY_LOG=
SERVER_ID=


########################################################
# ndbinstaller.sh SCRIPT PARAMETERS                    #
########################################################

FORCE_GENERATE_SSH_KEY=0
PARAM_NUM_NODES=
PARAM_CONFIG_DEFAULT=0
PARAM_NODEID=
PARAM_USERNAME=0
NDBD_PARAM_NO=0
MGM_HOST_NOT_SET=1
MGM_PORT_NOT_SET=1
PARAM_DEFAULT_INSTALL_NDB_DIR=0
PARAM_DEFAULT_INSTALL_MYSQL_DIR=0
PARAM_DEFAULT_MYSQL_SETTINGS=0
MYSQL_HOST_NOT_SET=1
CLEAN_INSTALL_DIR=0
SKIP_DOWNLOAD_MGM_SSH_KEY=0

########################################################
# Script Internal Configuration Params                 #
########################################################

LOCALHOST="127.0.0.1"


# Default values for user install actions
SYSTEM_INSTALL=1
SYMBOLIC_LINK=1
INSTALL_DB=1
GET_BINARIES=1
UNTAR=1
DEBUG=0
INITIAL=0
USERNAME=mysql
HOMEDIR=
INSTALL_BINARIES=1
REMOVE_EXPANDED_BINARIES_AFTER_INSTALL=0
START_WITH_SSH=0
ADD_MYSQLD=0
NUM_CORES=8

# failure codes returned by script
SUCCESS=0
FAILURE=-1
DIR_NOT_FOUND=-2
CONFIG_ERROR=-3
COULD_NOT_START_NDBS=-4
EXIT_SIGNAL_CAUGHT=-5
COULD_NOT_CREATE_USER=-6
COULD_NOT_CHANGE_PASSWD=-7

SCRIPTNAME=`basename $0`

# Default values for the generated startup scripts
NO_DAEMON=
NO_DAEMON_LAUNCH=

# Script names
INIT_START=init-cluster
NORMAL_START=start-cluster-with-recovery
CLUSTER_START=node.sh
NDBD_INIT=init-start-ndbd
NDBD_START=start-noinit-ndbd 
NDBD_STOP=stop-ndbd 
CLUSTER_SHUTDOWN=shutdown-cluster.sh
MGM_CLIENT_START=mgm-client.sh
ROLLING_RESTART=rolling-restart
MGMD_START=mgm-server-start
START_BACKUP=start-backup.sh
NDB_RESTORE=ndb-restore.sh
ENTER_SINGLE_USER_MODE=enter-singleuser-mode.sh
EXIT_SINGLE_USER_MODE=exit-singleuser-mode.sh
MEMORY_USAGE_SCRIPT=memory-usage.sh

# Temporary Scripts for Patching
MYSQLSTART_TMP="/tmp/mysql-start.sh"
MYSQL_SHUTDOWN_TMP="/tmp/mysql-shutdown.sh"

# Default values for cluster startup from script
MGM_STARTUP_TIME=15 
NDB_WAIT=300 #  seconds to wait for NDBDs to be started
NUM_NODES_TO_START=2
NUM_MYSQLS=0
NUM_MYSQL_BINS=0
# Num seconds to wait when testing to see if a cluster is already running
WAIT_CLUSTER_ALREADY_RUNNING=2
# Time to wait for a NDBD to restart during a rolling restart
NDBD_RESTART_TIMEOUT=300

# User Installation Options Variables
NUM_NODES=4
INSTALL_LOG="cluster_"installation-${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}".log"

# Used by 'wget' to download binaries
HTTP_PROXY=
HTTP_USER=
HTTP_PASS=
PROXY=

NON_INTERACT=0

# Script-Internal Installation Variables
NDB_NUM=
FINISHED_INSTALLDIRS=0
MOVE_BINARIES=0
START_SCRIPTS=
NDB_HOME=
MYSQL_DATA_DIR=
CHANGE_PORT_SCRIPT=
CHANGE_SOCKET_SCRIPT=
INSTALL_AS_DAEMON_HELP=
MYSQL_INSTALLED=
MYSQL_BINARIES_INSTALLED=
MYSQLD_STARTER=
MYSQLD_STOPPER=
LINUX_DISTRIBTUION=
NDBD_BIN=
NDBD_PROG=ndbmtd
#NDBD_PROG=ndbd


# Install action options
#INSTALL_COMPILE=0
INSTALL_LOCALHOST=1
INSTALL_MGM=2
INSTALL_NDB=3
INSTALL_MYSQLD=4
INSTALL_ANOTHER_MYSQLD_LOCALHOST=5
INSTALL_LOCALHOST_MYSQLD=6
INSTALL_ANOTHER_MYSQLD=7
INSTALL_ACTION=
NODEID=

###################################################################################################
# Sanity check for the script's current PATH
###################################################################################################

# Get the PATH where this script is stored
# If script ran as: ./$SCRIPT_NAME, then it's the working directory
CWD=`dirname $0`
if [ "$CWD" = "." ] ; then
    CWD=`pwd`
fi

i=0
i=$(($i+`echo "$CWD" | grep -c \ `))
i=$(($i+`echo "$CWD" | grep -c \*`))
i=$(($i+`echo "$CWD" | grep -c \\\\\\\\`))
i=$(($i+`echo "$CWD" | grep -c \"`))
i=$(($i+`echo "$CWD" | grep -c \'`))
i=$(($i+`echo "$CWD" | grep -c \\\``))
if [ $i -ne 0 ];
then
  echo "Path to current working directory contains critical characters"
  echo "like an asterisk, backslash, quotes or spaces. Since some system"
  echo "tools will not cope with such characters this script will abort now."
  echo "Unable to determine XFree86 Version. Stopping now."  
  echo ""
  exit 1
fi

####################################################################################################
# HELP STRINGS
####################################################################################################

README="
README for MySQL Cluster installed using ndbinstaller.\n
\n
NDBINSTALLER SCRIPTS\n
Bash shell scripts for starting, stopping, accessing and administering\n
your MySQL Cluster are located in:\n
./ndb/scripts\n
\n
Please read all of this README before starting/stopping your cluster,\n
as there are different scripts for the first startup of cluster,\n
and subsequent startups. Use the --help switch to find out more\n
information about the different scripts.\n
\n
\n
1.\n
move to the scripts directory:\n
cd ndb/scripts\n
\n
1. FIRST TIME START OF MYSQL CLUSTER \n
To start a 2-node cluster for the first time:\n
./init-start-cluster-2node.sh\n
\n
If instead, you want to start a 4-node cluster (for the first time) use:\n
./init-start-cluster-4node.sh\n
\n
These scripts perform an \"initial start\", where the contents of the\n
database are deleted. Be care not to call these scripts if you\n
have important data stored in the database.\n
\n
2. USE MGM CLIENT TO SEE IF CLUSTER IS WORKING PROPERLY\n
./mgm-client.sh -e show\n
\n
This should show you the following (2 ndbd nodes running, 1 mgmd running,\n
and a MySQL server connected to the cluster):\n
Connected to Management Server at: localhost:1186\n
Cluster Configuration\n
---------------------\n
[ndbd(NDB)]	2 node(s)\n
id=1	@127.0.0.1  (mysql-5.1.24 ndb-6.3.16, Nodegroup: 0, Master)\n
id=2	@127.0.0.1  (mysql-5.1.24 ndb-6.3.16, Nodegroup: 0)\n
\n
[ndb_mgmd(MGM)]	1 node(s)\n
id=63	@127.0.0.1  (mysql-5.1.24 ndb-6.3.16)\n
\n
[mysqld(API)]	9 node(s)\n
id=3	@127.0.0.1  (mysql-5.1.24 ndb-6.3.16)\n
\n
If this step failed, there was a problem starting your cluster.\n
\n
\n
3. CREATE A TABLE USING THE MYSQL CLIENT \n
./mysql-client-1.sh test\n
\n
mysql> create table test_ndb (id int) engine=ndbcluster;\n
\n
mysql> insert into test_ndb values (1),(2);\n
\n
mysql> show create table test_ndb\G\n
*************************** 1. row ***************************\n
       Table: test_ndb\n
Create Table: CREATE TABLE test_ndb (\n
  id int(11) DEFAULT NULL\n
) ENGINE=ndbcluster DEFAULT CHARSET=latin1\n
1 row in set (0.00 sec)\n
\n
# The above showed that we successfully created a ndb table \n
\n
4. SHUTTING DOWN MYSQL CLUSTER\n
./shutdown-cluster.sh\n
\n
This stops the ndbd nodes, the ndb_mgmd node and the mysqld node.\n
You can skip stopping the mysqld node using a switch.\n
\n
5. SUBSEQUENT STARTS OF MYSQL CLUSTER (after 1st time)\n
Non-first-time start of a 2-node cluster is performed using:\n
./start-noinit-cluster-2node.sh\n
\n
This script performs a \"normal start\", where the contents of the\n
database are not deleted. \n
 \n
---------------------\n
AUTHOR Jim Dowling	\n
Date: Sept 1, 2008\n
"

USE_BINARIES_HELP="
Use Existing Binaries Help\n
===========================================================================
\n
You can use the existing MySQL binaries that are installed on this machine\n
to run a Data Node, Management Server or MySQL Server.\n
Data Nodes and MySQL Servers will install their own data directory,  under\n
the \$NDB_HOME directory. So each Data Node or MySQL Server has its own\n
data directory, but Data Nodes and MySQL Servers can share the same copy of\n
the MySQL binaries.\n
\n
Use the existing binaries for this installation?"

CPU_HELP="
Which CPU Version Help\n
===========================================================================
\n
To find out the CPU type of this machine, type 'less /proc/cpuinfo'.\n
Data nodes and management servers must have the same type of CPU architecture.\n
So, you cannot mix x86 and powerpc nodes in a cluster.\n
It is not recommended to mix 32-bit and 64-bit data nodes in the same cluster.\n
Requirements: this installer expects that you have libc version 2.3 installed."



get_install_option_help()
{
  if [ $ROOTUSER -eq 1 ] ; then
    INSTALL_AS_DAEMON_HELP="You are installing cluster as root user. The ndb_mgmd and\n ndbd processes will start as daemon processes."
  else
    INSTALL_AS_DAEMON_HELP="You are installing cluster as normal (non-root) user. The ndb_mgmd and\n ndbd processes will start as user-level processes in '--no-daemon' mode."
  fi

INSTALL_OPTION_HELP="
Install Options Help\n
===========================================================================\n
A MySQL Cluster consists of up to 63 nodes:  1 or more Management Server, \n
(ndb_mgmd),  Data Nodes (ndbd), and Clients (MySQL Servers and/or NDBAPI apps).\n
$INSTALL_AS_DAEMON_HELP\n
\n
(1) Setup and start a localhost cluster. The cluster will run on this machine. \n
    \t1 ndb_mgmd, 2/4 ndbds, and 1 mysqld will be configured.\n
    \tScripts will generated that allow you to stop/start the cluster.\n
    \tYou can start the cluster running at the end of the installation.\n
(2) Add a MySQL Server ('mysqld') to an installed localhost cluster.\n
    \tRequirement: You have run installation step (1).\n
(3) Setup a distributed cluster. Installs a Management Server on the current\n
    \thost. This is the first step in installing a distributed cluster.\n
(4) Add a data node (the 'ndbd' process) to a distributed cluster.\n
    \tRequirement: You have run installation step (3) on this or  another host.\n
    \tRequirement: You know the 'connectstring' for the Management Server.\n
(5) Add a MySQL Server ('mysqld') to an installed distributed cluster.\n
    \tRequirement: You have run installation step (3).\n
    \tRequirement: You know the 'connectstring' for the Management Server."

}


#    \tYou will optionally be asked to install a MySQL Server.\n
#     \tData nodes store data in the Network Database (NDB).\n
REPLICA_HELP="
Number of Replicas Help\n
===========================================================================
\n
A replica is a copy of a partition in the cluster.\n
So, '2' replicas means '2' copies of the partition are stored in the cluster\n
(not 2 extra copies of an existing copy).\n
\n
The number of Replicas is currently 2. Accept?"


NODEID_HELP="The Id is a number that is assigned to a [NDBD] entry in the \$NDB_HOME/config-<NoOfNodes>Node.ini file. \n
Select the Id that matches the HostName on which you are trying to start the ndbd."

USERNAME_HELP="
UID (user id) for Running MySQL Cluster Help\n
===========================================================================
\n
The MySQL and NDB processes will be started and run with this user ID (uid). \n
\n
The default username that processes will be run as is '$USERNAME'.\n
Accept?"

DATA_MEMORY_HELP="
DataMemory Help\n
===========================================================================
\n
Reference:\n
\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html \n
You need enough free RAM to store Data Memory, Index Memory for all replicas.\n
Leave spare RAM (overhead) for other operating system processes.\n
\n
For example: \n
On a host with 2GB of RAM, as part of a 2-node cluster with 2 Replicas:\n
Data Memory could be 750MB, Index Memory could be 150MB.\n
This would leave 200MB overhead for Operating System and other processes.\n
\n
Note: if you want to do an online alter table, after entering single-user mode,\n
you will need enough spare memory to make a copy of your table in memory.\n
\n
Size of Data Memory for NDB nodes is 80MB.\n
Accept?"

index_memory_help_setup()
{

INDEX_MEMORY_HELP="
IndexMemory Help\n
===========================================================================
\n
Reference:\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
\n
You need enough free RAM to store Data Memory, Index Memory for all replicas.\n
Leave spare RAM (overhead) for other operating system processes.\n
\n
For example:\n
On a host with 2GB of RAM, as part of a 2-node cluster with 2 Replicas:\n
Data Memory could be 750MB, Index Memory could be 150MB.\n
This would leave 200MB overhead for Operating System and other processes.\n
\n
Note: if you want to do an online alter table, after entering single-user mode,\n
you will need enough spare memory to make a copy of your table in memory.\n
\n
Size of Index Memory for NDB nodes is $INDEX_MEMORY MB.\n
Accept?"
}

num_fragment_logfiles_help_setup()
{
NUM_FRAGMENT_LOGFILES_HELP="
NoOfFragmentLogFiles Help\n
===========================================================================
\n
References:\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-config-lcp-params.html\n
\n
The Redo Log is used when a node crashes to recover most its data.\n
The size of the Redo Log is configured by changing the 'NoOfFragmentLogFiles'. \n
The Redo Log is allocated in chunks of 64MB Fragment Log Files on local disk.\n
The default value for NoOfFragmentLogFiles is '8'.\n
This means 8 sets of four 16MB files (i.e., 8*64MB) for a total of 512MB. \n
That is, you need to have 512MB of free disk space.\n
If you have very high update rates (lots of writes/updates), you may need\n
to set NoFragmentLogFiles substantially higher, e.g., up to a value of 300.\n
\n
The NumberOfFragmentLogFiles is $NUM_FRAGMENT_LOGFILES (64*$NUM_FRAGMENT_LOGFILES = $REDO_LOG_SIZE MB)\n
Accept?"
}

DISK_CHECKPOINT_SPEED_HELP="
DiskCheckpointSpeed Help\n
===========================================================================
\n
References:\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
\n
The amount of data,in bytes per second, that is sent to disk during a \n
local checkpoint.\n
You can check how fast your hard disk can write to disk by checking the\n
manufacturer's specification for you hard disk or by using a program\n
such as Bonnie++. For debian/ubutunu:\n
>apt-get install bonnie++       >bonnie++\n
\n
In any case, the algorithms for writing the local checkpoint will adapt\n
the speed of writing to load on the cluster.\n
\n
The default value is 10M (10 megabytes per second).\n
Accept?"


DISK_CHECKPOINT_SPEED_IN_RESTART_HELP="
DiskCheckpointSpeedInRestart Help\n
===========================================================================
\n
References:\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
\n

The amount of data, in bytes per second, that is sent to disk during a \n
local checkpoint that is performed when a node restarts.\n
\n
You can check how fast your hard disk can write to disk by checking the\n
manufacturer's specification for you hard disk or by using a program\n
such as Bonnie++. For debian/ubutunu: >apt-get install bonnie++\n
\n
In any case, the algorithms for writing the local checkpoint will adapt\n
the speed of writing to load on the cluster.\n
\n
The default value is ${DISK_CHECKPOINT_SPEED_IN_RESTART}M (${DISK_CHECKPOINT_SPEED_IN_RESTART} megabytes per second).\n
Accept?"


DISK_SYNC_SIZE_HELP="
DiskSyncSize Help\n
===========================================================================
\n
References:\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
\n
This is the maximum number of bytes to store in the Redo Buffer before\n
flushing the buffer to a local checkpoint file.\n
\n
If your hard disk is a bottleneck, then increasing the size of this parameter\n
may help throughput when writing to disk.\n
However, in the event of a node crash, the transactions in the Redo Buffer\n
that were not written to disk will be lost (although they will be available in\n
replica nodes).\n
\n
The default value is 4M (4 megabytes).\n
Accept?"

REDO_BUFFER_HELP="
RedoBuffer Help\n
===========================================================================
\n
References:\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-ndbd-definition.html\n
\n
If you have a write-intensive database or a slow disk subsystem, it is\n
recommended that you increase the size of the redo buffer from the default\n
value of 8Mb to 16Mb.\n
\n
The RedoBuffer sets the size of the buffer in which the REDO log is written.\n
This buffer is used as a front end to the file system when writing REDO log\n
records to disk. If this buffer is too small, the NDB storage engine issues\n
error code 1221 (REDO log buffers overloaded).\n
\n
The default value is 8M (8 megabytes).\n
Accept?"



DELETE_DIR_HELP="
\nAbove Directory already exists.\n 
\n
You have already installed the NDB files or MySQL Binaries in this directory.\n
\n
If you have already installed a Mmgmt Server, and you want to install a\n
Data Node, you can install it in this directory - do not delete it.\n
\n
Otherwise, do not delete this directory if you have important data here. \n
For example, if you have already installed a Management Server,\n
and have stored important data in it, this step may\n
cause you to lose that data.\n
If you delete this directory, you will do a clean install.\n
\n
Delete this directory?\n
$FULL_DIR"

MYSQL_PORT_HELP="
MySQL Port Help\n
===========================================================================\n
A port can be used to connect to a MySQL server.\n
\n
For example:\n
mysql --protocol=tcp --port=3306 -u root -p\n
\n
The port number for the mysqld is set to default (port $MYSQL_PORT).\n
Accept?"

MYSQL_SOCKET_HELP="
MySQL Socket Help\n
===========================================================================\n
The socket can be used to connect to a MySQL server.\n
For example:\n
mysql -S /tmp/mysql.sock -u root -p\n
\n
The default location for the MySQL socket is:\n
$MYSQL_SOCK\n
Accept?"

BINARY_LOG_HELP="
MySQL Binary Log Help\n
===========================================================================\n
A binary log of updates to this MySQL server is required to support\n
replication from this server. This server will be a Replication Master.\n
\n
The binary log will be stored in the MySQL server's data directory and will\n
consume an increasing amount of disk space over time. You will need to\n
write/download a script to rotate the binary logs to prevent them growing\n
over time.\n
\n
\n
Should binary logging be enabled for this MySQL Server?\n
Accept?"


SSH_HELP="
SSH Help\n
===========================================================================\n
Requirements: \n
These programs must installed on all hosts:\n
ssh-keygen, scp, ssh.\n
\n
Generating a ssh-key makes it easier to initialise or start the cluster.\n
Instead of having to logon to all hosts in the cluster and run the init or\n
start scripts for all Data Nodes and Management Servers in the cluster, you\n
will only need to run a single init or start script from the Management Server\n
Host.\n
\n
A ssh-key is used to run a program on a remote host under a specified\n
username. The ssh-key is used to allow you run a single script on the\n
management server to initialise or start the management server (on the same\n
host), and all the Data Nodes (on remote hosts) in the cluster. To start the\n
Data Nodes, the single startup script calls the data node startup scripts on\n
the remote hosts using ssh. The single scripts require that the ssh-key for\n
the management server is installed on the Data Nodes.\n
\n
Do you want to generate a ssh-key?"

AUTHORIZED_KEYS_HELP="
SSH Authorized Keys Help\n
===========================================================================\n
A public key file should have been installed on the management server host under\n
a user account. Normally, it is stored in the path:\n
/home/\$USERNAME/.ssh/id_rsa.pub\n
The id_rsa.pub file is appended to the authorized_keys file on this host\n
to enable the \$USERNAME on the management\n
to execute scripts that start Data Nodes on remote hosts.\n
\n
Is /home/\$USERNAME/.ssh/id_rsa.pub the correct path for the public key file?"


####################################################################################################
# Replication Strings
####################################################################################################

binary_log_disable()
{
BINARY_LOG="#log-bin=mysql-bin"
# generate a server-id between 0 and 4,999 for the master
# generate a server-id between 5,000 and 10,000 for the slave
MASTER_ID=`expr $RANDOM % 5000`
SLAVE_ID=`expr $RANDOM % 10000`
SLAVE_ID=`expr $SLAVE_ID + 5000`
SERVER_ID="#server-id=$MASTER_ID"
}


binary_log_enable()
{
BINARY_LOG="log-bin=mysql-bin"
# generate a server-id between 0 and 4,999 for the master
# generate a server-id between 5,000 and 10,000 for the slave
MASTER_ID=`expr $RANDOM % 5000`
SLAVE_ID=`expr $RANDOM % 10000`
SLAVE_ID=`expr $SLAVE_ID + 5000`
SERVER_ID="server-id=$MASTER_ID"
}

slave_init()
{
#Create a slave account on the master (cluster) MySQL Server  with the appropriate privileges
# ${MYSQL_HOST}
# ${MGM_HOST}
# run this on the master

REPLICATION_ACCOUNT_CREATE="
#GRANT REPLICATION SLAVE ON *.* TO '$USERID'@'$MYSQL_MASTER' IDENTIFIED BY '$PASSWORD';"

# Write a warning that the my.cnf file is written in cleartext to a file in the MySQL Server's data directory
REPLICATION_SLAVE="
#server-id=2222
#master-host=$MYSQL_MASTER
#master-port=3306
#master-user=$USERID
#master-password=$PASSWORD
"
}

####################################################################################################
# Test for user-id (if root) for generated cluster startup scripts
####################################################################################################

#TODO: make this a shell script in util dir
test_userid()
{
TEST_USERID="
USERID=\`id | sed -e 's/).*//; s/^.*(//;'\`
if [ \"X\$USERID\" != \"X$USERNAME\" ]; then
 if [ \"X\$USERID\" = \"Xroot\" ]; then
   echo \"\"
   echo \"You started cluster as user: 'root'.\"
   echo \"You should start cluster as user: '$USERNAME'.\"
   echo \"If you continue, you will change ownership of database files\"
   echo \"from '$USERNAME' to 'root'.\"
# TODO: return -2
 else
   echo \"\"
   echo \"You started the cluster as user: '\$USERID'.\"
   echo \"You should start the cluster as user: '$USERNAME'.\"
   echo \"If you continue, you will change ownership of database files\"
   echo \"from '$USERNAME' to '\$USERID'.\"
# TODO: return -2
 fi
 
 echo \"\"
start_as_wrong_user() 
{
  echo -n \"Do you really want to start the cluster as user '$USERID'? y/n/h(help) \"
  read ACCEPT
  case \$ACCEPT in
   y | Y)
      ;;
   n | N)
      echo \"\"
      echo \"Bye..\"
      echo \"\"
      exit 1
      ;;
    *)
      echo \"\"
      echo -n \"Please enter 'y' or 'n'.\" 
      start_as_wrong_user
      ;;
   esac
}
start_as_wrong_user


fi"
}

already_running()
{
ALREADY_RUNNING_CLUSTER="
  echo \"\" 
  echo \"Testing to see if a cluster is already running on host '$CONNECTSTRING' ...\" 
  echo \"\" 
  ${MYSQL_BINARIES_DIR}/bin/ndb_mgm -c $CONNECTSTRING -t $WAIT_CLUSTER_ALREADY_RUNNING -e show 1> /dev/null

  if [ \$? -eq 0 ] ; then
      echo \"\"      
      echo \"A management server is already running on $CONNECTSTRING\" 
      echo \"\" 	
      exit 2
  fi
"
}


get_connectstring_skip_mysqlds()
{

GET_CONNECTSTRING_SKIP_MYSQLDS="
MGM_CONN=$CONNECTSTRING
SKIP_MYSQLDS=0

while [ \$# -gt 0 ]; do
  case \"\$1\" in
    -h|--help|-help)
              echo \"usage: <prog> [ -c | --connectstring MGMD_HOST:MGMD_PORT ] [ -s|--skip-mysqlds ]\"
	      echo \"\"
	      echo \"connectstring is set to $CONNECTSTRING\"
	      exit 0 
	      ;;
    -c|--connectstring)
              shift
	      MGM_CONN=\$1
	      break 
	      ;;
    -s|--skip-mysqlds)
              SKIP_MYSQLDS=1
	      ;;
	   * )
              echo \"Unknown option '\$1'\" 
              exit -1
  esac
  shift       
done
"
}

get_connectstring()
{

GET_CONNECTSTRING="
MGM_CONN=$CONNECTSTRING
while [ \$# -gt 0 ]; do
  case \"\$1\" in
    -h|--help|-help)
              echo \"usage: <prog> [ -c | --connectstring MGMD_HOST:MGMD_PORT ] \"
	      echo \"\"
	      echo \"connectstring is set to $CONNECTSTRING\"
	      exit 0 
	      ;;
    -c|--connectstring)
              shift
	      MGM_CONN=\$1
	      break 
	      ;;
	   * )
              echo \"Unknown option '\$1'\" 
              exit -1
  esac
  shift       
done
"
}

get_mgm_client_connectstring()
{

MGM_CLIENT_CONNECTSTRING="
MGM_CONN=$CONNECTSTRING
PARAMS=
EXEC=
while [ \$# -gt 0 ]; do
  case \"\$1\" in
    -h|--help|-help)
              echo \"usage: <prog> [ -c | --connectstring MGMD_HOST:MGMD_PORT ] ] [ -e [command] ] \"
	      echo \"\"
	      echo \"Default connectstring parameter = $CONNECTSTRING\"
	      echo \"\"
	      echo \"To view the state of the cluster (which nodes are connected), type:\"
	      echo \"./mgm-client.sh -e show\"
	      echo \"\"
	      exit 0 
	      ;;
    -e)
              shift
	      EXEC=\"-e\"
              while [ \$# -gt 0 ]; do
		  PARAMS=\"\$PARAMS \$1\"
                  shift
              done
	      break 
	      ;;
    -c|--connectstring)
              shift
	      MGM_CONN=\$1
	      break 
	      ;;
	   * )
              echo \"Unknown option '\$1'\" 
              exit -1
  esac
  shift       
done
"
}


###################################################################################################
# INTERRUPT HANDLER FUNCTIONS
###################################################################################################

# called by TrapBreak if interrupt signal is handled
CleanUpTempFiles() 
{

case $INSTALL_ACTION in
 $INSTALL_MYSQLD|$INSTALL_ANOTHER_MYSQLD_LOCALHOST|$INSTALL_LOCALHOST_MYSQLD|$INSTALL_ANOTHER_MYSQLD)
    if [ -d $MYSQL_DATA_DIR ] ; then
	echo ""
	echo "Cleaning up...."
	echo "Removing the mysql data directory:"
	echo "$MYSQL_INSTALL_DIR"
	echo ""
	# The install directory is the parent directory that needs to be removed
	rm -rf ${MYSQL_INSTALL_DIR}
    fi
  ;;
esac
echo ""

test -e $MYSQLSTART_TMP && rm -rf $MYSQLSTART_TMP
test -e $MYSQL_SHUTDOWN_TMP && rm -rf $MYSQL_SHUTDOWN_TMP

}

# called if interrupt signal is handled
TrapBreak() 
{
  trap "" HUP INT TERM
  # if ndbd or ndb_mgmd or mysqld are still running, kill them
#  [ -n "$MYSQL_PID" ] && kill -TERM "$MYSQL_PID" 2>/dev/null
#  [ -n "$pid" ] && killall "ndbd" 2>/dev/null
  
  echo -e "\n\nInstallation cancelled by user!"
  exit_error $EXIT_SIGNAL_CAUGHT
}


###################################################################################################
# USER INPUT HANDLER FUNCTIONS
###################################################################################################

# Takes help strings as parameter
# $1 = "Is this
entry_ok() 
{
  echo -n " y/n/h(help) "
  read ACCEPT
  case $ACCEPT in
   y | Y | yes | Yes | YES)
      return 1
      ;;
   n | N | no | No | NO)
      return 0
      ;;
   h | H | help | Help | HELP)
     # add escaped new-line characters
      if [ $# -eq 0 ] ; then
	echo -e "\nThere is no help available for this installation step."
      else
        clear
        echo -en $@
      fi
      entry_ok $@
      return $?
      ;;
    *)
      echo -n "Please enter 'y', 'n', or 'h (help)'." $ECHO_OUT
      entry_ok $@
      ;;
   esac

  return 1
}
 
# $1 = accept phrase (what to accept)
# caller reads $ENTERED_STRING global variable for result
enter_string() 
{
     echo "$1" $ECHO_OUT
     read ENTERED_STRING
}

###################################################################################################
# SCREEN CLEAR FUNCTIONS
###################################################################################################

clear_screen()
{
 if [ $NON_INTERACT -eq 0 ] ; then
   echo "" $ECHO_OUT
   echo "Press ENTER to continue" $ECHO_OUT
   read cont < /dev/tty
 fi 
 clear
}

clear_screen_no_skipline()
{
 if [ $NON_INTERACT -eq 0 ] ; then
    echo "Press ENTER to continue" $ECHO_OUT
    read cont < /dev/tty
 fi 
 clear
}

###################################################################################################
# ERROR HANDLER FUNCTIONS
###################################################################################################

# $1 = String describing error
mkdir_error()
{
 echo "" $ECHO_OUT
 echo "Failure: couldn't create the directory:" $ECHO_OUT
 echo "$1" $ECHO_OUT
 echo "This script does not perform recursive directory creation. " $ECHO_OUT
 echo "Do not use '~/' as a path, use the full path (e.g., /home/user/..). " $ECHO_OUT
 echo "Exiting...." $ECHO_OUT
 echo "" $ECHO_OUT
 exit_error
}

# $1 = String describing error
exit_error() 
{
  #CleanUpTempFiles

  echo "" $ECHO_OUT
  echo "Error number: $1" $ECHO_OUT
  echo "Exiting $PRODUCT $VERSION installer." $ECHO_OUT
  echo "" $ECHO_OUT
  exit 1
}

# fix format of dir string, if necessary
# $1=directory to be fixed
fix_dir () {
  FIXDIR="$1"
  FIRSTCHAR=`expr "$FIXDIR" : '\(.\).*'`
  if [ "$FIRSTCHAR" != '/' ]; then
    currentdir=`pwd`
    echo "$currentdir/$FIXDIR" $ECHO_OUT
  else
    echo "$1" $ECHO_OUT
  fi
}


#####################################################################################################
# INSTALLER FUNCTIONS
#####################################################################################################

# download binaries from MySQL mirror site
download_binaries()
{
cd $CWD

BASE_VER="${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}-${PLATFORM}-${CPU}"
#BASE_VER="${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}-${PLATFORM}-${CPU}-glibc23"
#BASE_VER="mysql-cluster-gpl-${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}-ndb-${NDB_VERSION_MAJOR}.${NDB_VERSION_MINOR}.${NDB_VERSION_REV}${RELEASE}"
DISTRO="${MIRROR}${BASE_VER}.tar.gz${MIRROR_DIR}"
#DISTRO="${MIRROR}${BASE_VER}/${VERSION}.tar.gz${MIRROR_DIR}"
#DISTRO="${MIRROR}${MIRROR_DIR}${BASE_VER}.tar.gz"

FINISHED=0
while [ $FINISHED = 0 ]
do
   echo "" $ECHO_OUT
   echo "Trying to download the MySQL binaries from the mirror:" $ECHO_OUT
   echo "$1" $ECHO_OUT
   echo "" $ECHO_OUT
   echo "HTTP proxy = ${http_proxy}" $ECHO_OUT
   
   echo "wget $PROXY $HTTP_USER $HTTP_PASS $DISTRO -O ${VERSION}.tar.gz" $ECHO_OUT
   clear_screen
   wget $PROXY $HTTP_USER $HTTP_PASS $DISTRO -O ${VERSION}.tar.gz

   if [ $? -eq 0 ] ; then
       FINISHED=1
   else
       FINISHED=-1
   fi
done

if [ $FINISHED -eq -1 ] ; then
   echo "" $ECHO_OUT
   echo "Failure. Could not download the MySQL binaries from the following mirrors:" $ECHO_OUT
   echo "$DISTRO" $ECHO_OUT
   echo "Try downloading the binaries using your Browser to: $CWD" $ECHO_OUT
   echo "Then re-run ./$SCRIPTNAME" $ECHO_OUT
   echo "" $ECHO_OUT
   echo "Exiting......." $ECHO_OUT
   exit_error
fi


#if [ "$INSTALL_ACTION" = "$INSTALL_COMPILE" ] ; then
     echo "" $ECHO_OUT
     echo "You now need to build the binaries from the sources just downloaded." $ECHO_OUT
     echo "" $ECHO_OUT
#fi


}

#
# Generic function to create the installation directories (ndb, mysql) for root/normal-user
# Very hacked function - needs to know what you're installing ndb, mysql data, mysql binaries
#
# $1=$NDB_DIR or $MYSQL_BINARIES_DIR or $MYSQL_INSTALL_DIR
# $2=$NDB_INSTALL_DIR or $MYSQL_INSTALL_DIR or $MYSQL_DATA_DIR
# $3=informational text
# $4=$NDB_VERSION or $VERSION
# $5=Accept default directory
#
install_dir() {

 FULL_DIR=$1
 INSTALL_DIR=$2

FINISHED_INSTALLDIRS=0
while [ $FINISHED_INSTALLDIRS -eq 0 ] ; do

 echo "" $ECHO_OUT
 echo "The name of the $3 directory must be:" $ECHO_OUT
 echo "$4" $ECHO_OUT
 echo "" $ECHO_OUT
 echo "We recommend installing this directory in the following path:" $ECHO_OUT
 echo "$FULL_DIR" $ECHO_OUT

 if [ "$5" != "1" ] ; then
 echo -n "Do you agree to install in the above path?"
 if [ $NON_INTERACT -eq 0 ] ; then
     entry_ok "The $3 files will be installed to directory:\n $FULL_DIR/$4\n\nIn a later step of this installation, you will be able to create a shorter directory name using a symbolic link to the $3 directory.\n\nDo you agree to install in the above path?\n"
 else 
     echo "Installing in default directory.\n"
     eval false  # Non-zero exit status to take default installation path
 fi
  if [ $? -eq 0 ] ; then
    echo "" $ECHO_OUT
    echo "Enter the complete installation path. Do not use ~/ in the path!" $ECHO_OUT
    echo "" $ECHO_OUT
    if [ $ROOTUSER -eq 1 ] ; then
      echo "E.g., '/usr/local' installs the distribution in" $ECHO_OUT
      echo "      '/usr/local/$VERSION'" $ECHO_OUT
    else
      echo "E.g., '/home/YOUR_USERNAME/.mysql' installs the distribution in" $ECHO_OUT
      echo "      '/home/YOUR_USERNAME/.mysqld/$VERSION'" $ECHO_OUT
    fi
    echo "" $ECHO_OUT
    printf 'Enter installation path: '
    read dir
    INSTALL_DIR=$dir
  fi
 fi
  if [ $ROOTUSER -eq 1 ] ; then
  # root users must enter a full pathname
      FULL_DIR=$INSTALL_DIR/$4
  else
  # fix the user-level dir if necessary
      FULL_DIR=`fix_dir "$INSTALL_DIR"`
      FULL_DIR=$FULL_DIR/$4
  fi
  
  # set return value using global variable
  WHICH_INSTALL=$4
  # compare version start string with "mysql" 
  TO_UPDATE=${WHICH_INSTALL:0:5}

  if [ "$TO_UPDATE" = "mysql" ] ; then
      MYSQL_INSTALL_DIR=$INSTALL_DIR 
      MYSQL_BINARIES_DIR=$FULL_DIR
      NDBD_BIN=$MYSQL_BINARIES_DIR/bin/$NDBD_PROG
  fi

  TO_UPDATE=${WHICH_INSTALL:0:3}
  if [ "$TO_UPDATE" = "ndb" ] ; then
	  NDB_INSTALL_DIR=$INSTALL_DIR  
	  NDB_DIR=$FULL_DIR
  fi

  TO_UPDATE=${WHICH_INSTALL:0:6}
  if [ "$TO_UPDATE" = "var" ] ; then
     MYSQL_INSTALL_DIR=$INSTALL_DIR  
  fi


  # Now create directory if it doesn't exist
  # MYSQL_INSTALL_DIR is the parent of MYSQL_BINARIES_DIR
  if [ ! -d $INSTALL_DIR ] ; then

      if [ "$5" != "1" ] ; then
       echo "" $ECHO_OUT
       echo "Directory : $INSTALL_DIR" $ECHO_OUT
       echo -n "Create this directory?" $ECHO_OUT
       if [ $NON_INTERACT -eq 0 ] ; then
         entry_ok "Directory does not exist: \n $INSTALL_DIR \n It is recommended that you create it."
       else 
          eval false  # Non-zero exit status to take default installation path
       fi
       if [ $? -eq 1 ] ; then
	    FINISHED_INSTALLDIRS=1
            mkdir -p $INSTALL_DIR
            if [ $? -ne 0 ] ; then
              mkdir_error $INSTALL_DIR
            fi
       else
            FINISHED_INSTALLDIRS=0
       fi
      else
            mkdir -p $INSTALL_DIR
            if [ $? -ne 0 ] ; then
              mkdir_error $INSTALL_DIR
            fi
	    FINISHED_INSTALLDIRS=1
      fi
   else
	    FINISHED_INSTALLDIRS=1
   fi

   # FULLDIR IS THE CONVENTION NAME FOR THE INSTALL (mysql-version, ndb-version, data)
   if [ -d $FULL_DIR ] ; then
       echo "" $ECHO_OUT
       echo "WARNING: this install directory already exists:" $ECHO_OUT
       echo "$FULL_DIR" $ECHO_OUT

       echo "" $ECHO_OUT
       TO_UPDATE=${WHICH_INSTALL:0:3}
       if [ "$TO_UPDATE" = "ndb" ] && [ $INSTALL_ACTION -eq $INSTALL_NDB ] ; then
	   echo "If you have installed a MgmServer or NDBD on this host, then do NOT delete" $ECHO_OUT
	   echo "this directory: a data node will be installed in the same directory safely." $ECHO_OUT
	   echo "" $ECHO_OUT
	   echo -n "Delete the contents of this directory and do a clean install?" $ECHO_OUT
       else
	   echo -n "Delete the contents of this directory and do a clean install?" $ECHO_OUT
       fi
       if [ $NON_INTERACT -eq 0 ] ; then
         entry_ok "=========================================================\n" $FULL_DIR $DELETE_DIR_HELP
       else 
         eval false  # Non-zero exit status to take default installation path
       fi
 
       if [ $? -eq 1 ] || [ $CLEAN_INSTALL_DIR -eq 1 ] ; then
             rm -rf $FULL_DIR
  	     if [ $? -ne 0 ] ; then
  		echo "" $ECHO_OUT
  		echo "Failure when attempting to delete $FULL_DIR" $ECHO_OUT
  		echo "" $ECHO_OUT
  		echo "Exiting....." $ECHO_OUT
  		echo "" $ECHO_OUT
                  exit_error
             fi
  	     FINISHED_INSTALLDIRS=1
       else
	 echo "Installing in this directory (may overwrite some existing files)!" $ECHO_OUT	      
         FINISHED_INSTALLDIRS=2
       fi
   fi
  
done


echo "" $ECHO_OUT
echo "$3 installation directory will be:" $ECHO_OUT
echo "$FULL_DIR" $ECHO_OUT

if [ $FINISHED_INSTALLDIRS -eq 1 ] ; then
   mkdir $FULL_DIR
   if [ $? -ne 0 ] ; then
     mkdir_error $FULL_DIR
   fi
fi

}



###################################################################################################
# FUNCTION TO GENERATE my.cnf FILE
###################################################################################################

make_my_cnf()
{

echo "  
#################################################
# This my.cnf file was generated by ndbinstaller.
# You can edit this file, and changes will be 
# picked up the next time you restart mysqld using
# the 'start-mysqld-X.sh' script, or when you
# restart the whole cluster using
# 'start-no-init-cluster-Xnode.sh'.
#################################################

[mysqld]
$MYSQL_USER
basedir         = $MYSQL_BINARIES_DIR
datadir         = $MYSQL_DATA_DIR
port            = $MYSQL_PORT
socket          = $MYSQL_SOCK
bind-address    = $MYSQL_HOST

# use NDB storage engine and make NDB the default table type
ndbcluster
default-storage-engine = ndbcluster
ndb-cluster-connection-pool=2

#################################################
# optimizations to improve performance of NDB
#################################################
#
ndb-use-exact-count=0

# flush the adaptive-send buffer immediately 
ndb-force-send=0

# allow indexes to be used on ndb nodes (rather than joins performed in MySQL Server)
engine-condition-pushdown=1

# use the cluster connection pool to reduce cluster connection setup time
ndb-cluster-connection-pool=4

# Log more data at MySQL Server about ndb
ndb-extra-logging=0

# for autoincrement, size of prefetching 
ndb-autoincrement-prefetch-sz=256

#################################################
# Other [mysqld] params
#################################################

# do not include innodb engine 
#$SKIP_INNODB

# replication settings go here
$SERVER_ID
$BINARY_LOG

#################################################
# mysql_cluster connection params
#################################################

[mysql_cluster]
# set connectstring to ndb management server (used by all executables)
ndb-connectstring=$CONNECTSTRING

" > $MY_CNF $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $MY_CNF" $ECHO_OUT
 exit_error
fi

}


###################################################################################################
# FUNCTION TO GENERATE config-2node.ini and config-4node.ini FILES
###################################################################################################

# $1=NoNodes, $2=NoMYSQLs
make_config_ini()
{

if [ $# -ne 1 ] ; then
	exit_error "Incorrect num params to make_config_ini"
fi

COUNT=0
NDBD_DEFNS=
while [ $COUNT -lt $1 ] ; do

NDBD_DEFNS="$NDBD_DEFNS
[NDBD]"

NDBD_DEFNS="$NDBD_DEFNS
HostName=${NDB_HOST[$COUNT]}"

COUNT=`expr $COUNT + 1`

NDBD_DEFNS="$NDBD_DEFNS
DataDir=$NDB_LOGS_DIR
FileSystemPath=$NDB_INSTALL_DIR/ndb/$NDB_DATADIR/$COUNT
NodeId=$COUNT
#Assign this data node to a CPU-ID
#LockExecuteThreadToCPU=
#LockMaintThreadToCPU=
"

done


echo " 
#
# This config.ini file was generated by ndbinstaller.
#

[NDBD DEFAULT]
#Used by ndbmdmt to determine the number of LQH threads
MaxNoOfExecutionThreads=$NUM_CORES

#More flexible than MaxNoOfExecutionThreads
#Threadconfig=main={cpubind=0},ldm={count=8,cpubind=1,2,3,4,13,14,15,16},io={count=4,cpubind=5,6,17,18},rep={cpubind=7},recv={count=2,cpubind=8,19}

#The number of copies of the data stored on different nodes in the cluster
NoOfReplicas=$NUM_REPLICAS

#The amount of main memory (RAM) used to store columns and ordered indexes in tables, plus some overhead
DataMemory=$DATA_MEMORY

#The amount of main memory (RAM) used to hash indexes in tables, plus some overhead
IndexMemory=$INDEX_MEMORY

#The amount of disk space (NoOfFragmentLogFiles * 64MB) used to store the Redo Log (used for node recovery)
NoOfFragmentLogFiles=$NUM_FRAGMENT_LOGFILES

#The size of a Redo Log file on disk.
FragmentLogFileSize=$FRAGMENT_LOGFILE_SIZE

#The speed at which LocalCheckpoints (LCP) are written to disk
DiskCheckpointSpeed=$DISK_CHECKPOINT_SPEED

#The speed at which LocalCheckpoints (LCP) are written to disk, as part of a node recovery 
DiskCheckpointSpeedInRestart=$DISK_CHECKPOINT_SPEED_IN_RESTART

#The size of the RedoLog Buffer in memory; Reduce for improved disk throughput (but slower recovery time)
DiskSyncSize=$DISK_SYNC_SIZE

#The size of the RedoBuffer used to buffer writes to the disk subsystem. Increase for high write-rate or slow disk.
RedoBuffer=$REDO_BUFFER

#The maximum time in ms that is permitted to lapse between operations in the same transaction before the transaction is aborted.
TransactionInactiveTimeout=$TRANSACTION_INACTIVE_TIMEOUT

#Time in ms between global checkpoint groups are flushed from memory to disk
#TimeBetweenGlobalCheckpoints=$TIME_BETWEEN_GLOBAL_CHECKPOINTS

#Time in ms between local checkpoints of memory to local disk. Increase this if system is highly loaded to improve node restart times.
#TimeBetweenLocalCheckpoints=$TIME_BETWEEN_LOCAL_CHECKPOINTS

#Time in ms between replication events sent up from cluster
#TimeBetweenEpochs=$TIME_BETWEEN_EPOCHS
#TimeBetweenEpochsTimeout=32000

#Heartbeating
#HeartbeatIntervalDbDb=1500
#HeartbeatIntervalDbApi=1500

#Represents the number of seconds between memory usage reports written to the cluster log
#MemReportFrequency=$MEMORY_REPORT_FREQUENCY
#LogLevelStartup=15
#LogLevelShutdown=15
#LogLevelCheckpoint=8
#LogLevelNodeRestart=15

#Disk Data
#SharedGlobalMemory=20M
#DiskPageBufferMemory=64M
#BatchSizePerLocalScan=512

#This prevents ndbd processes and DB memory from being swapped out to disk
#LockPagesInMainMemory=$LOCK_PAGES_IN_MAIN_MEMORY

#If you have lots of tables (>1000), then you may get 'Error 773' and need to increase this (25 would be reasonable)
#StringMemory=$STRING_MEMORY

#
# TRANSACTION PARAMETERS
#

#The number of transaction records available at each data node. Increase this if you have a higher number of concurrent transactions
#MaxNoOfConcurrentTransactions=4096
MaxNoOfConcurrentTransactions=32768

#The number of operation records available at each data node. Increase this if you have a higher number of concurrent operations
#MaxNoOfConcurrentOperations=100000
MaxNoOfConcurrentOperations=200000

#The number of operation records available at each data node for queries that use a hash index. Increase this if you have a higher number of concurrent operations
#MaxNoOfConcurrentIndexOperations=16384
MaxNoOfConcurrentIndexOperations=65536


#
# Scans and Buffering
#

# Max number of parallel scans. Max value is 500.
#MaxNoOfConcurrentScans=$MAX_NO_OF_CONCURRENT_SCANS

#Note: 'alter table' requires 3 times the number of attributes that are in the original table. 
MaxNoOfAttributes=$MAX_NO_OF_ATTRIBUTES

#A table is required not just for a table, but also for every unique hash index and every ordered index. Maximum value is 20320, minimum is 8.
#MaxNoOfTables=$MAX_NO_OF_TABLES

#Each ordered index requires 10KB of memory.
#MaxNoOfOrderedIndexes=$MAX_NO_OF_ORDERED_INDEXES

#Each unique hash index requires 15KB of memory.
#MaxNoOfUniqueHashIndexes=$MAX_NO_OF_UNIQUE_HASH_INDEXES

#Replication, unique index operations, backups and order index operations require trigger objects.
#MaxNoOfTriggers=768

# 
#Backup Parameters
#The following must hold when updating backup params:
# 1. BackupDataBufferSize >= BackupWriteSize + 188KB
# 2. BackupLogBufferSize >= BackupWriteSize + 16KB
# 3. BackupMaxWriteSize >= BackupWriteSize
#

#Increase if slow disk subsystem when making a backup. 
#BackupDataBufferSize=2M

#Increase if slow disk subsystem when making a backup. 
#BackupLogBufferSize=2M

#Default size of msgs written to disk. This value must be less than or equal to BackupMaxWriteSize.
#BackupWriteSize=32000

#Max size of msgs written to disk. This value must be greater than or equal to than BackupWriteSize.
BackupMaxWriteSize=1M

#Frequency of Backups
#BackupReportFrequency=$BACKUP_REPORT_FREQUENCY

TimeBetweenWatchdogCheckInitial=60000
#
# Real-time params
#

#Setting this to '1' enables real-time scheduling of ndb threads.
#RealtimeScheduler=0

#Time in microseconds for threads to be executed in the scheduler before being sent.
#SchedulerExecutionTimer=50

#Time in microseconds for threads to be executed in the scheduler before sleeping.
#SchedulerSpinTimer=0

#Whether a ndbd process should halt or be restarted if there is an error in it.
#StopOnError=true

#where disk-based user data will be used for the cluster
Diskless=false

#Data nodes attempt to use O_DIRECT when writing LCPs, backups and redo logs.
#Should be disabled for 2.4 or older kernels, enabled for 2.6 or higher kernels
ODirect=true


[MYSQLD DEFAULT]

[NDB_MGMD DEFAULT]

[TCP DEFAULT]
SendBufferMemory=2M
ReceiveBufferMemory=2M

 
# Management Server
[NDB_MGMD]
NodeId=$MGMD_ID
HostName=$MGM_HOST
PortNumber=$MGM_PORT
DataDir=$NDB_LOGS_DIR
LogDestination=FILE:filename=$NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME,maxsize=10000000,maxfiles=6
ArbitrationRank=1


$NDBD_DEFNS

# Setup node IDs for mySQL API-servers (clients of the cluster)
[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

[MYSQLD]

" > $NDB_DIR/config.ini

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $NDB_DIR/config.ini" $ECHO_OUT
 exit_error
fi

}
 

###################################################################################################
# FUNCTION TO GENERATE start/stop cluster FILES
###################################################################################################

writeable_scripts_dir()
{
 if [ ! -w  "${NDB_DIR}/scripts/" ] ; then
  echo "" $ECHO_OUT
  echo "Failure: write permission denied for directory: ${NDB_DIR}/scripts " $ECHO_OUT
  echo "Enable write permission for this directory by running 'chmod +w ${NDB_DIR}/scripts'" $ECHO_OUT
  exit_error
 fi
}

# $1=numOfNodes, $2={init,noinit}, $3={localhost,distributed}
make_start_cluster_script()
{

  if [ $# -lt 2 ] ; then
   echo "" $ECHO_OUT
   echo "Too few parameters to script." $ECHO_OUT
   exit -1
  fi
  NUM_TO_START=$1
  START_TYPE=
  INIT=
  DISTRIBUTED=0


CHECK_ON_STARTUP=
  if [ "$2" = "init" ] ; then
      START_TYPE="$INIT_START"
      INIT="--initial"
      CHECK_ON_STARTUP="

echo \"Your are initializing and starting the MySQL Cluster database.\"
echo \"If you have already initialised the Cluster, exit this script\"
echo \"and run the 'start-noinit-' script, instead.\"
echo \"\"

really_start() 
{
  echo -n \"Do an initial start of NDB? This will DELETE any existing data!  y/n/h(help) \"
  read ACCEPT
  case \$ACCEPT in
   y | Y)
      ;;
   n | N)
      exit 1
      ;;
    *)
      echo \"\"
      echo \"Please enter 'y' or 'n'.\" 
      really_start
      ;;
   esac
}
if [ \$SKIP_USER_CHECK -eq 0 ] ; then
 really_start
fi
      "
      
  elif [ "$2" = "noinit" ] ; then
      START_TYPE="$NORMAL_START"
  else
   echo "Parameter \$2 is invalid." $ECHO_OUT
   exit -1
  fi

  if [ "$3" = $LOCALHOST ] ; then
      DISTRIBUTED=0
  elif [ "$3" = "distributed" ] ; then
      DISTRIBUTED=1
  else
   echo "Parameter \$3 is invalid." $ECHO_OUT
   exit -1
  fi
 
if [ $ROOTUSER -eq 0 ] ; then
# > filename 2>&1 is the same as &> filename
 LOGGING="> $NDB_LOGS_DIR/mgmd-stdout-1.log 2>&1"
fi
  
test_userid

already_running


echo "#!/bin/sh 

SKIP_NDBDS=0
SKIP_MYSQLDS=0
SKIP_USER_CHECK=0

while [ \$# -gt 0 ]; do
  case \"\$1\" in
    -h|--help|-help)
              echo \"usage: <prog> [--skip-ndbds (do not start data nodes)] [--skip-mysqlds (do not start mysql servers)] [-sc (delete old configurations)] [-f skip user check]\"
	      echo \"\"
	      echo \"connectstring is set to $CONNECTSTRING\"
	      exit 0 
	      ;;
    -f|--force)
              SKIP_USER_CHECK=1
	      ;;
    -sc)
              echo \"Deleting old configurations from /$NDB_DIR/$MGM_DATADIR/\"
              rm -rf ../$NDB_DIR/$MGM_DATADIR/*
	      ;;
    --skip-ndbds)
              SKIP_NDBDS=1
	      ;;
    --skip-mysqlds)
              SKIP_MYSQLDS=1
	      ;;
	   * )
              echo \"Unknown option '\$1'\" 
              exit -1
  esac
  shift       
done

$ALREADY_RUNNING_CLUSTER

$TEST_USERID  

$CHECK_ON_STARTUP

echo \"Cluster Startup may take a few minutes.\"  


echo \"Truncating the cluster log file: ${NDB_LOGS_DIR}/${CLUSTER_LOG_FILENAME}\"
rm $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME 

echo \"Starting the Management Server .....\"

if [ -e $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME ] ; then
    SIZE_CL=\`cat $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME | wc -l\`
else
    SIZE_CL=0
fi

#remove memory of old configurations
#echo "If you want to remove warnings for incompatible configuration changes, run 'rm -rf ../$NDB_DIR/$MGM_DATADIR/*'"

$MYSQL_BINARIES_DIR/bin/ndb_mgmd -f $NDB_DIR/config.ini $NO_DAEMON --configdir=$NDB_DIR/$MGM_DATADIR --reload $LOGGING $NO_DAEMON_LAUNCH 

if [ \$? -ne 0 ] ; then
  echo \"Problem starting the Management Server.\"
  echo \"Please read the logs in:\"
  echo \"$NDB_LOGS_DIR.\"
  echo \"$NDB_DIR/mgmd_1\"
  exit 1
fi

MGMD_STARTED=0

echo \"Waiting for ndb_mgmd to start\"
    

MGMD_TIMEOUT=200
MGMD_COUNT=0
while [ \$MGMD_STARTED -eq 0 ] ; do

  if [ -e $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME ] ; then
    UPDATED_SIZE_CL=\`cat $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME | wc -l\`
  else
    UPDATED_SIZE_CL=0
  fi
    
  if [ \$SIZE_CL -lt \$UPDATED_SIZE_CL ] ; then
	MGMD_STARTED=1;
	echo \"\"
  else

      if [ \${MGMD_COUNT} -eq 0 ] ; then
	  echo -n \"Seconds left before timeout: \"
      fi

      echo -n \"\`expr \${MGMD_TIMEOUT} - \${MGMD_COUNT}\` \"
      sleep 1
      MGMD_COUNT=\`expr \${MGMD_COUNT} + 1\`

      if [ \$MGMD_COUNT -gt \$MGMD_TIMEOUT ] ; then
	  MGMD_STARTED=2;
      fi
  fi

done

  if [ \$MGMD_STARTED -ne 1 ] ; then
   echo \"\"
   echo \"Failure when starting the 'ndb_mgmd'.\"
   echo \"Please check for errors in your configuration file or report a bug.\"
   echo \"\"
   echo \"You now need to kill the ndb_mgmd process.\"
   echo \"To find the proceses-id of ndb_mgmd: 'ps -ef | grep ndb_mgmd | grep $USERNAME' \"
   echo \"To kill the process: kill -9 [process-id]\"
   exit 2
  fi


sleep 1

" > $NDB_DIR/scripts/${START_TYPE}.sh $ECHO_OUT
  
  if [ $? -ne 0 ] ; then
   echo "" $ECHO_OUT
   echo "Failure: could not create file $NDB_DIR/scripts/${START_TYPE}.sh" $ECHO_OUT
   exit_error
  fi

  COUNT=0
  LOGGING=
  while [ $NUM_TO_START -gt $COUNT ] ; do

     NODEID=`expr ${COUNT} + 1`
     if [ $ROOTUSER -eq 0 ] && [ $DISTRIBUTED -eq 0 ] ; then
	 LOGGING="> ${NDB_LOGS_DIR}/ndb-stdout-${NODEID}.log 2>&1"
     fi
     if [ $DISTRIBUTED -eq 0 ] ; then

       echo "

if [ \$SKIP_NDBDS -eq 0 ] ; then
   if [ ! -e $NDBD_BIN ] ; then
      echo \"Error: could not find file: $NDBD_BIN\"
      exit 3
   fi


  echo \"Starting Data Node ${NODEID} on Localhost.\"

  $NDBD_BIN -c $CONNECTSTRING --ndb-nodeid=${NODEID} $INIT $NO_DAEMON $LOGGING $NO_DAEMON_LAUNCH

  if [ \$? -ne 0 ] ; then
    echo \"Problem starting a Data Node.\"
    echo \"Please read the logs in:\"
    echo \"$NDB_LOGS_DIR\"
    echo \"$NDB_DIR/mgmd_1\"
  fi

else

  echo \"Skipping starting ndbd.\"

fi

sleep 1" >> $NDB_DIR/scripts/${START_TYPE}.sh $ECHO_OUT

     else

       if [ $START_WITH_SSH -eq 1 ] ; then
       # DISTRIBUTED -eq 1 
	     if [ "$2" = "init" ] ; then
		 echo "
echo \"\"

if [ \$SKIP_NDBDS -eq 0 ] ; then
    echo \"Starting a Data Node on host: ${NDB_HOST[$COUNT]} \"
    echo \"For username: ${USERNAME} \"
  ssh ${USERNAME}@${NDB_HOST[$COUNT]} ${NDB_INSTALL_DIR}/ndb/scripts/${NDBD_INIT}-${NODEID}.sh

  if [ \$? -ne 0 ] ; then
    echo \"Problem starting a Data Node on host ${NDB_HOST[$COUNT]}.\"
    echo \"Please read the logs on host ${NDB_HOST[$COUNT]} in directories:\"
    echo \"$NDB_LOGS_DIR\"
    echo \"$NDB_DIR/data_dir/${NODEID}\"
  fi
else
  echo \"Skipping starting ndbd.\"
fi

sleep 1" >> $NDB_DIR/scripts/${START_TYPE}.sh $ECHO_OUT
	     else
		 echo "

echo \"Starting a Data Node on host: ${NDB_HOST[$COUNT]}   \"

ssh ${USERNAME}@${NDB_HOST[$COUNT]} ${NDB_INSTALL_DIR}/ndb/scripts/${NDBD_START}-${NODEID}.sh

if [ \$? -ne 0 ] ; then
  echo \"Problem starting a Data Node on host ${NDB_HOST[$COUNT]}.\"
  echo \"Please read the logs on host ${NDB_HOST[$COUNT]} in directories:\"
  echo \"$NDB_LOGS_DIR\"
  echo \"$NDB_DIR/data_dir/${NODEID}\"
fi
sleep 1" >> $NDB_DIR/scripts/${START_TYPE}.sh $ECHO_OUT

	     fi
	 fi
      fi
     COUNT=`expr $COUNT + 1`
  done
  
if [ $DISTRIBUTED -eq 0 ] || [ $START_WITH_SSH -eq 1 ] ; then
echo "
echo \"Waiting for the cluster to be ready by calling:\"
echo \"ndb_waiter -c $CONNECTSTRING --timeout=$NDB_WAIT\"
echo \"This can take a few minutes...\"

$MYSQL_BINARIES_DIR/bin/ndb_waiter -c $CONNECTSTRING --timeout=$NDB_WAIT 2>&1 > /dev/null

if [ \$? -ne 0 ] ; then
    echo \"Error when waiting on the cluster to be ready.\"
    echo \"Exiting...\"
    exit 3
fi

$MYSQLD_START_SCRIPT \$SKIP_MYSQLDS

sleep 1

$NDB_DIR/scripts/$MGM_CLIENT_START -e show

exit 0
" >> $NDB_DIR/scripts/${START_TYPE}.sh $ECHO_OUT

fi

}


make_start_mysqlds_script()
{

make_util_dir

MYSQLD_START_SCRIPT=$NDB_DIR/scripts/util/start-mysqlds.sh

echo "#!/bin/sh 

echo \"Starting MySQL Servers....\"

"  >  $MYSQLD_START_SCRIPT $ECHO_OUT

chmod +x $MYSQLD_START_SCRIPT


if [ $ROOTUSER -eq 1 ] ; then
    chown -R $USERNAME $NDB_DIR/scripts/util/
fi

}

make_mgm_scripts()
{
writeable_scripts_dir

make_start_mysqlds_script

make_start_cluster_script $NUM_NODES "init" "distributed"
make_start_cluster_script $NUM_NODES "noinit" "distributed"

}


make_localhost_scripts()
{
writeable_scripts_dir

make_start_mysqlds_script

make_start_cluster_script 2 "init" $LOCALHOST
make_start_cluster_script 2 "noinit" $LOCALHOST
#make_start_cluster_script 4 "init" $LOCALHOST
#make_start_cluster_script 4 "noinit" $LOCALHOST

}

##########################################################################################
# Function to generate script for shutting down a cluster
##########################################################################################

make_shutdown_cluster()
{

get_connectstring_skip_mysqlds

echo "#!/bin/sh 

$GET_CONNECTSTRING_SKIP_MYSQLDS

$MYSQL_BINARIES_DIR/bin/ndb_mgm -c \$MGM_CONN -e \"shutdown\"
"  > $NDB_DIR/scripts/$CLUSTER_SHUTDOWN $ECHO_OUT


if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $NDB_DIR/scripts/$CLUSTER_SHUTDOWN" $ECHO_OUT
 exit_error
fi

get_mgm_client_connectstring

echo "#!/bin/sh 

$MGM_CLIENT_CONNECTSTRING

$MYSQL_BINARIES_DIR/bin/ndb_mgm -c \$MGM_CONN \$EXEC \$PARAMS
" > $NDB_DIR/scripts/$MGM_CLIENT_START $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $NDB_DIR/scripts/$MGM_CLIENT_START" $ECHO_OUT
 exit_error
fi

}

##########################################################################################
# Function to generate script for starting a NDBD
##########################################################################################

make_start_ndbd() 
{

writeable_scripts_dir

NODE_STARTER="$NDB_DIR/scripts/${NDBD_INIT}-${NODEID}.sh"
NODE_INITER="$NDB_DIR/scripts/${NDBD_START}-${NODEID}.sh"
NODE_STOPPER="$NDB_DIR/scripts/${NDBD_STOP}-${NODEID}.sh" 

if [ $ROOTUSER -eq 0 ] ; then
 LOGGING="> ${NDB_LOGS_DIR}/ndb-stdout-${NODEID}.log 2>&1"
fi

test_userid
get_connectstring

echo "#!/bin/sh
ID=${NODEID}
WATCHDOG=\`cat ../../logs/ndb_\${ID}.pid\`
PID=\`expr \$PID + 1\`
echo \"Killing process-id \$PID and \$WATCHDOG ...\\n\" >> ../../logs/ndb_\${ID}_out.log
echo \"Killing process-id \$PID and \$WATCHDOG ...\\n\" 
kill \$WATCHDOG
kill \$PID
exit 0" > $NODE_STOPPER $ECHO_OUT


echo "#!/bin/sh 

$TEST_USERID

$GET_CONNECTSTRING

echo \"Starting Data Node ${NODEID}.\"
echo \"\"

$NDBD_BIN -c \$MGM_CONN --initial --ndb-nodeid=${NODEID} $NO_DAEMON $LOGGING $NO_DAEMON_LAUNCH
RES=\`echo \$?\`
exit $RES
"  > $NODE_STARTER $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $NODE_STARTER" $ECHO_OUT
 exit_error
fi

echo "#!/bin/sh 

$TEST_USERID  

$GET_CONNECTSTRING

echo \"Starting Data Node ${NODEID}.\"

$NDBD_BIN -c \$MGM_CONN --ndb-nodeid=${NODEID} $NO_DAEMON $LOGGING $NO_DAEMON_LAUNCH
RES=\`echo \$?\`
exit \$RES
"  > $NODE_INITER $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $NODE_INITER" $ECHO_OUT
 exit_error
fi

}


##########################################################################################
# Function to generate script for starting a Management Server
##########################################################################################

# ${1} the number of hosts in the cluster
make_start_mgmd() 
{

writeable_scripts_dir

MGMD_START=$NDB_DIR/scripts/mgm-server-start-${1}node.sh
MGMD_STOP=$NDB_DIR/scripts/mgm-server-stop-${1}node.sh

if [ $ROOTUSER -eq 0 ] ; then
 LOGGING="> ${NDB_LOGS_DIR}/mgmd-stdout-${1}.log 2>&1"
# > /dev/null"
fi

test_userid
already_running

echo "#!/bin/sh
ID=${MGMD_ID}
PID=\`cat ../../logs/ndb_\${ID}.pid\`
echo \"Killing mgm server with process-id \$PID \" >> ../../logs/ndb_\${ID}_out.log
echo \"\"
echo \"Killing mgm server with process-id \$PID \" 
kill \$PID
exit \$?" > $MGMD_STOP $ECHO_OUT


echo "#!/bin/sh 

$TEST_USERID

$ALREADY_RUNNING_CLUSTER

if [ ! -e $MYSQL_BINARIES_DIR/bin/ndb_mgmd ] ; then
  echo \"Error: could not find file: $MYSQL_BINARIES_DIR/bin/ndb_mgmd\"
  exit 3
fi

$MYSQL_BINARIES_DIR/bin/ndb_mgmd -f  $NDB_DIR/config.ini $NO_DAEMON --configdir=$NDB_DIR/mgmd${1} --reload $LOGGING $NO_DAEMON_LAUNCH
RES=\`echo \$?\`
if [ \$RES -ne 0 ] ; then
    echo \"\"
    echo \"Error when starting the management server: \$?.\"
    echo \"\"
    exit 2
fi
exit \$RES
"  > $MGMD_START $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $MGMD_START" $ECHO_OUT
 exit_error
fi

 chmod +x $MGMD_START

}



##########################################################################################
# Function to generate script for starting a MYSQLD
##########################################################################################
make_mysqld() 
{
MYSQLD_STOPPER=$NDB_DIR/scripts/stop-mysqld-$1.sh
MYSQLD_STARTER=$NDB_DIR/scripts/start-mysqld-$1.sh
MYSQLD_START_SCRIPT=$NDB_DIR/scripts/util/start-mysqlds.sh

writeable_scripts_dir

test_userid


echo "#!/bin/sh 

$TEST_USERID  

echo \"Test if a mysql server is already running on this host.\"

MYSQL_SOCKET=\`/var/lib/mysql-cluster/ndb-7.2.8/scripts/util/get-mysqld-$1-socket.sh\`
$MYSQL_BINARIES_DIR/bin/mysqladmin -S \$MYSQL_SOCKET -s -u root ping 
# Don't redirect error, as this will give a '0' return result &> /dev/null
if [ \$? -eq 0 ] ; then
 echo \"A MySQL Server is already running at socket $MYSQL_SOCKET. Not starting another MySQL Server at this socket.\"
 exit 1
fi

$MYSQL_BINARIES_DIR/bin/mysqld --defaults-file=$MY_CNF 1> $NDB_LOGS_DIR/mysql-stdout-$1.log 2>&1 &
RES=\`echo \$?\`
exit \$RES
"  > $MYSQLD_STARTER $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $MYSQLD_STARTER" $ECHO_OUT
 exit_error
fi

echo "#!/bin/sh 

$TEST_USERID  


MYSQL_SOCKET=\`/var/lib/mysql-cluster/ndb-7.2.8/scripts/util/get-mysqld-$1-socket.sh\`
$MYSQL_BINARIES_DIR/bin/mysqladmin -S \$MYSQL_SOCKET -u root --wait=30 shutdown

RES=\`echo \$?\`
exit \$RES
"  > $MYSQLD_STOPPER $ECHO_OUT


if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $MYSQLD_STOPPER" $ECHO_OUT
 exit_error
fi

chmod +x $MYSQLD_STARTER
chmod +x $MYSQLD_STOPPER

}

make_util_dir()
{
if [ ! -d $NDB_DIR/scripts/util ] ; then
    mkdir $NDB_DIR/scripts/util
    if [ $? -ne 0 ] ; then
	echo "" $ECHO_OUT
	echo "Failure: could not create directory $NDB_DIR/scripts/util" $ECHO_OUT
	exit_error
    fi
fi
}


make_get_mysql_port()
{

make_util_dir

CHANGE_PORT_SCRIPT=$NDB_DIR/scripts/util/get-mysqld-$1-port.sh

echo "#!/bin/sh 

MYSQL_PORT=\`grep ^port $MY_CNF | sed -e 's/port.*= //'\`

echo \"\$MYSQL_PORT\"
"  >  $CHANGE_PORT_SCRIPT $ECHO_OUT

chmod +x $CHANGE_PORT_SCRIPT


CHANGE_SOCKET_SCRIPT=$NDB_DIR/scripts/util/get-mysqld-$1-socket.sh

echo "#!/bin/sh 

MYSQL_SOCKET=\`grep ^socket $MY_CNF | sed -e 's/socket.*= //'\`

echo \"\$MYSQL_SOCKET\"
"  >  $CHANGE_SOCKET_SCRIPT $ECHO_OUT

chmod +x $CHANGE_SOCKET_SCRIPT


if [ $ROOTUSER -eq 1 ] ; then
    chown -R $USERNAME $NDB_DIR/scripts/util/
fi

}

##########################################################################################
# Function to generate script for starting a mysql client
##########################################################################################
# $1 = mysqld number make_start_mysql_client() 
make_start_mysql_client()
{
START_MY_CLIENT=$NDB_DIR/scripts/mysql-client-$1.sh
ADD_USER_SCRIPT=$NDB_DIR/scripts/add-mysql-user.sh

writeable_scripts_dir

echo "#!/bin/sh

if [ \"\$1\" = \"-h\" ] ; then
   echo \"Usage: mysql-client-$1.sh [-s] [database_name]\"
   echo \"[-S] connects the mysql client using a socket, instead of tcp protocol.\"
   echo \"[-e] executes and SQL statement and the client then exits.\"
   exit 0
fi
SOCKET=0
if [ \"\$1\" = \"-S\" ] ; then
    shift
    SOCKET=1
fi

EXECUTE_SQL=0
if [ \"\$1\" = \"-e\" ] ; then
    shift
    EXECUTE_SQL=1
fi

if [ \$SOCKET -eq 1 ] ; then
    MYSQL_SOCKET=\`$CHANGE_SOCKET_SCRIPT\`
   if [ \$EXECUTE_SQL -eq 1 ] ; then
    $MYSQL_BINARIES_DIR/bin/mysql -u root -S \$MYSQL_SOCKET -e \"\$@\"
   else
    $MYSQL_BINARIES_DIR/bin/mysql -u root -S \$MYSQL_SOCKET 
   fi
else
    PORT=\`$CHANGE_PORT_SCRIPT\`
    $MYSQL_BINARIES_DIR/bin/mysql -u root --protocol=tcp -h $MYSQL_HOST --port=\$PORT \$EXECUTE_SQL
fi

"  > $START_MY_CLIENT $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $NDB_DIR/scripts/mysql-client-$1.sh" $ECHO_OUT
 exit_error
fi

chmod +x $START_MY_CLIENT


echo "#!/bin/sh
if [ \$# -ne 2 ] ; then
echo \"Usage: <prog> username password\"
exit 1
fi

COMMAND=\"\\\"GRANT ALL PRIVILEGES ON \\\*.\\\* to '\$1'@'%' IDENTIFIED BY '\$2'\\\"\"
./mysql-client-1.sh -S -e \$COMMAND

exit \$?
"  > $ADD_USER_SCRIPT $ECHO_OUT

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file $NDB_DIR/scripts/$ADD_USER_SCRIPT" $ECHO_OUT
 exit_error
fi

chmod +x $ADD_USER_SCRIPT

}

##########################################################################################
# Function to generate script for performing a rolling restart
# Precondition: management script must have been run first!
# $1 = number of nodes to restart!
##########################################################################################
make_rolling_restart() 
{
ROLLING_RESTART="$NDB_DIR/scripts/rolling-restart-$1nodes.sh"


writeable_scripts_dir

MGMD_STARTED_STRING="Node $MGMD_ID: Node $MGMD_ID Connected"

echo "#!/bin/sh

  case \$1 in
    \"-h\"|\"--help\"|\"-help\")
   echo \"\"
   echo \"Usage: prog_name [restart-timeout default=$NDBD_RESTART_TIMEOUT]\"
   echo \"\"
   echo \"Restarts a cluster to effect any configuration changes to a cluster\"
   echo \"Restarts the management server first, then restarts the data nodes, in turn.\"
   echo \"\"
   echo \"Pass a higher restart-timeout value if data nodes do not complete a restart\"
   echo \"within $NDBD_RESTART_TIMEOUT seconds - increase this value for large databases.\"
   echo \"\"
   echo \"\"
   exit 0
      ;;      
  esac 


 RESTART_TIMEOUT=$NDBD_RESTART_TIMEOUT
if [ \"\$1\" != \"\" ] ; then
 RESTART_TIMEOUT=\$1
fi

  echo \"Restarting Management Server...\"
  $MYSQL_BINARIES_DIR/bin/ndb_mgm -c $CONNECTSTRING -e \"$MGMD_ID restart\"

  if [ \$? -ne 0 ] ; then
      echo \"\"
      echo \"Failure: could not restart management server. Failure Code: $?\"
      echo \"\"
      exit $MGMD_ID
  fi

  MGMD_COUNT=0
  MGMD_STARTED=0
  while [ \$MGMD_STARTED -eq 0 ] ; do

    FOUND=\`tail -30 $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME | grep \"$MGMD_STARTED_STRING\"\`
    if [ \"\$FOUND\" != \"\" ] ; then
	MGMD_STARTED=1;
	echo \"\"
    else
	if [ \${MGMD_COUNT} -eq 0 ] ; then
	    echo -n \"Seconds until timeout of MGMD $N restart: \"
	fi

	echo -n \"\`expr \${RESTART_TIMEOUT} - \${MGMD_COUNT}\` \"
	sleep 1
	MGMD_COUNT=\`expr \${MGMD_COUNT} + 1\`
	   
	if [ \$MGMD_COUNT -gt \${RESTART_TIMEOUT} ] ; then
	    MGMD_STARTED=2;
	fi
    fi
  done

  if [ \$MGMD_STARTED -ne 1 ] ; then
      echo \"\"
      echo \"Failure when re-starting the 'mgmd'.\"
      echo \"\"
      echo \"Restart the 'mgmd' or the entire cluster.\"
      echo \"\"
      exit 2
  else
      echo \"\"
      echo \"Restart of MGMD completed.\"
      echo \"\"
  fi

# Sleep, waiting for ndb_mgmd to connect to all nodes in cluster
   sleep 5

"  > $ROLLING_RESTART $ECHO_OUT

   N=$1
   while [ $N -ne 0 ] ; do
      NDB_STARTED_STRING="Node $N: Start phase 101 completed"
echo "
   echo \"Restarting Data Node $N...\"
   $MYSQL_BINARIES_DIR/bin/ndb_mgm -c $CONNECTSTRING -e \"$N restart\"
   if [ \$? -ne 0 ] ; then
	echo \"\"
	echo \"Failure: could not restart data node $N. Failure Code: $?\"
	echo \"\"
	exit $N
   fi

   NDBD_COUNT=0
   NDBD_STARTED=0
   while [ \${NDBD_STARTED} -eq 0 ] ; do

     FOUND=\`tail -300 ${NDB_LOGS_DIR}/${CLUSTER_LOG_FILENAME} | grep \"${NDB_STARTED_STRING}\"\`
     if [ \"\$FOUND\" != \"\" ] ; then
	 NDBD_STARTED=1;
	 echo \"\"
     else
	 if [ \${NDBD_COUNT} -eq 0 ] ; then
	     echo -n \"Seconds until timeout of NDBD $N restart: \"
	 fi

	 echo -n \"\`expr \${RESTART_TIMEOUT} - \${NDBD_COUNT}\` \"
	 sleep 1
	 NDBD_COUNT=\`expr \${NDBD_COUNT} + 1\`
	   
	 if [ \${NDBD_COUNT} -gt \${RESTART_TIMEOUT} ] ; then
	   NDBD_STARTED=2;
	 fi
     fi
   done

   if [ \$NDBD_STARTED -ne 1 ] ; then
       echo \"\"
       echo \"Failure when re-starting the 'ndbd'.\"
       echo \"\"
       echo \"Restart the 'ndbd' or the entire cluster.\"
       echo \"\"
       exit 2
   else
       echo \"\"
       echo \"Restart of NDBD $N completed.\"
       echo \"\"
   fi
   
# Sleep, waiting for NDBD to connect to all nodes in cluster
  sleep 5

      "  >> $ROLLING_RESTART $ECHO_OUT
      N=`expr $N - 1`
   done

   echo "
exit 0
   "  >> $ROLLING_RESTART $ECHO_OUT


if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file: $ROLLING_RESTART" $ECHO_OUT
 exit_error
fi

chmod +x $ROLLING_RESTART

}


##########################################################################################
# Function to generate script for performing a rolling restart
# Precondition: management script must have been run first!
# $1 = number of nodes to restart!
##########################################################################################
make_single_user_mode() 
{

ENTER_SINGLE_USER_MODE="$NDB_DIR/scripts/$ENTER_SINGLE_USER_MODE"
EXIT_SINGLE_USER_MODE="$NDB_DIR/scripts/$EXIT_SINGLE_USER_MODE"

writeable_scripts_dir

echo "#!/bin/sh

usage()
{
   echo \"\"
   echo \"Usage: prog_name NODE-ID\"
   echo \"\"
   echo \"MySQL Cluster enters single-user mode, where the only MySQL Server or API user\"
   echo \"that can access the cluster has a nodeID of NODE-ID.\"
   echo \"MySQL Cluster must be in single-user mode\"
   echo \"in order to convert columns or whole tables from in-memory to disk-based.\"
   echo \"Other operations that require single-user mode can be found here:\"
   echo \"http://dev.mysql.com/doc/refman/5.1/en/mysql-cluster-single-user-mode.html\"
   echo \"\"
}

  case \$1 in
    \"-h\"|\"--help\"|\"-help\")
      usage
      exit 0
      ;;      
      *)
      # do nothing
      ;;
  esac 

  if [ \$# -ne 1 ] || [ \$1 -lt 1 ] || [ \$1 -gt 255 ] ; then
      usage
      exit 1
  fi

  $MYSQL_BINARIES_DIR/bin/ndb_mgm -c $CONNECTSTRING -e \"ENTER SINGLE USER MODE \$1\"

" > $ENTER_SINGLE_USER_MODE

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file: $ENTER_SINGLE_USER_MODE" $ECHO_OUT
 exit_error
fi

chmod +x $ENTER_SINGLE_USER_MODE


echo "#!/bin/sh

  $MYSQL_BINARIES_DIR/bin/ndb_mgm -c $CONNECTSTRING -e \"EXIT SINGLE USER MODE \$1\"

" > $EXIT_SINGLE_USER_MODE

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file: $EXIT_SINGLE_USER_MODE" $ECHO_OUT
 exit_error
fi

chmod +x $EXIT_SINGLE_USER_MODE

}

##########################################################################################
# Function to generate script for performing a backup of the cluster
# Precondition: management script must have been run first!
# $1 = number of nodes to restart!
##########################################################################################
make_start_backup() 
{
START_BACKUP="$NDB_DIR/scripts/start-backup.sh"

writeable_scripts_dir


echo "#!/bin/sh

BACKUP_DIR=\"$NDB_DIR/backup/\"

  case \$1 in
    \"-h\"|\"--help\"|\"-help\")
   echo \"\"
   echo \"Usage: prog_name [-d BACKUP-DIR \(default=$NDB_DIR/backup\)]\"
   echo \"\"
   echo \"Creates a backup of the cluster, and stores it in BACKUP-DIR.\"
   echo \"Copies backups from all data-nodes to the management serverb.\"
   echo \"You need to authenticate at all data nodes to copy their backup files.\"
   echo \"\"
   exit 0
      ;;      
    \"-d\")
         shift
         BACKUP_DIR=\"$1\"
         ;;
  esac 

  echo \"Starting backup...\"
  $MYSQL_BINARIES_DIR/bin/ndb_mgm -c $CONNECTSTRING -e \"start backup\"

  if [ \$? -ne 0 ] ; then
      echo \"\"
      echo \"Failure: could not start backup. Failure Code: \$?\"
      echo \"\"
      exit \$?
  fi

# Sleep, waiting for ndb_mgmd to connect to all nodes in cluster
   sleep 5

   exit 0
"  > $START_BACKUP $ECHO_OUT

chmod +x $START_BACKUP

COPY_BACKUPS="$NDB_DIR/scripts/copy-backup-to-mgm-server.sh"

echo "#!/bin/bash

BACKUP_DIR=\"$NDB_DIR/backup\"
BACKUP_ID=1
NDB_DATA_DIR=\"$NDB_DIR/ndb_data\"

  case \$1 in
    \"-h\"|\"--help\"|\"-help\")
         echo \"\"
         echo \"Usage: prog_name [-b ID \(default=1\)] [-d BACKUP-DIR \(default=$NDB_DIR/backup\)]\"
         echo \"\"
         echo \"Copies backups, with optionally supplied backup id from data-nodes to mgm server.\"
         echo \"You need to authenticate at all data nodes to copy their backup files.\"
         echo \"\"
         exit 0
         ;;      
    \"-d\")
         shift
         BACKUP_DIR=$1
         ;;
    \"-b\")
         shift
         BACKUP_ID=$1
         ;;
  esac 

  BACKUP_FILE=\"BACKUP-\$BACKUP_ID.tgz\"
  BACKUP_FILES=

  HOST[0]=\"${NDB_HOST[0]}\" ;   HOST[1]=\"${NDB_HOST[1]}\"
  HOST[2]=\"${NDB_HOST[2]}\" ;   HOST[3]=\"${NDB_HOST[3]}\"
  HOST[4]=\"${NDB_HOST[4]}\" ;   HOST[5]=\"${NDB_HOST[5]}\"
  HOST[6]=\"${NDB_HOST[6]}\" ;   HOST[7]=\"${NDB_HOST[7]}\"
  HOST[8]=\"${NDB_HOST[8]}\" ;   HOST[9]=\"${NDB_HOST[9]}\"
  NUM=0

  while [ \$NUM -lt $NUM_NODES ] ; do

    node=\"\${HOST[\$NUM]}\"
    # directory numbers start from '1'
    NUM=\`expr \$NUM + 1\`

    echo \"\"
    echo \"Creating a .tgz archive of backup-id \$BACKUP_ID at node-id \$NUM:\"
    
    # tar/zip up backup at ndbd
    TAR_ZIP=\"cd \$NDB_DATA_DIR && tar zcf \$NUM-\$BACKUP_ID.tgz \$NUM\/BACKUP\"
    
    ssh ${USERNAME}@\${node} \${TAR_ZIP}

    if [ \$? -ne 0 ] ; then
      echo \"\"
      echo \"Failure: Could not find directory or run tar successfully at \${node}. Failure Code: \$?\"
      echo \"\"
      exit \$?
    fi

    # copy archive to backup dir
     echo \"Scp'ing the archived backup-id \$BACKUP_ID from node-id \$NUM to the mgm server:\"
     
     if [ ! -d \$BACKUP_DIR ] ; then
       mkdir \$BACKUP_DIR
       if [ \$? -ne 0 ] ; then
          echo \"\"
          echo \"Failure: Could not create backup dir: \$BACKUP_DIR. Failure Code: \$?\"
          echo \"\"
          exit \$?
       fi
     fi

     scp ${USERNAME}@\${node}:\${NDB_DATA_DIR}\/\$NUM-\$BACKUP_ID.tgz \${BACKUP_DIR}

     if [ \$? -ne 0 ] ; then
       echo \"\"
       echo \"Failure: could not scp backup archive from \${node}. Failure Code: \$?\"
       echo \"\"
       exit \$?
     fi

     BACKUP_FILES=\"\$BACKUP_FILES \$NUM-\$BACKUP_ID.tgz\"
  done

  cd \$BACKUP_DIR
  tar zcf \$BACKUP_FILE \$BACKUP_FILES

  if [ \$? -ne 0 ] ; then
      echo \"\"
      echo \"Failure: Could not created single backup archive. Failure Code: \$?\"
      echo \"\"
      exit \$?
  fi

  for f in \$BACKUP_FILES ; do
    rm \$f
  done

   echo \"\"
   echo \"\"
   echo \"The backup is now stored in:\"
   echo \" \$BACKUP_DIR/\$BACKUP_FILE\"
   echo \"\"
   echo \"\"
   exit 0
"  > $COPY_BACKUPS $ECHO_OUT

chmod +x $COPY_BACKUPS

}


##########################################################################################
# Function to generate a script for restoring a backup of the cluster
# $1 = number of nodes to restart!
##########################################################################################
make_ndb_restore()
{
NDB_RESTORE="$NDB_DIR/scripts/ndb-restore.sh"

echo "#!/bin/sh
usage()
{
   echo \"\"
   echo \"Usage: prog_name -b BACKUP-ID [-n NODE-ID] <PATH_TO_BACKUP_FILES>\"
   echo \"\"
   echo \"Restores the data for MySQL Cluster from a NODE-ID and a BACKUP-ID\"
   echo \"http://dev.mysql.com/doc/refman/5.1/\"
   echo \"\"
}

NODEID=
BACKUPID=

while [ \$# -gt 0 ]; do
  case \$1 in
    \"-h\"|\"--help\"|\"-help\")
      usage
      exit 0
      ;;      
    \"-n\")
      shift
      NODEID=\$1
      ;;      
    \"-b\")
      shift
      BACKUPID=\$1
      ;;      
      *)
      echo \"Error: Invalid switch or parameter.\"
      usage
      exit 0
      ;;
  esac 

  shift
done

  if [ \"\$BACKUPID\" = \"\" ] ; then
      echo \"Error: Backup-id not specified.\"
      usage
      exit 1
  fi

# TODO: BACKUP FOR ALL NODE-IDS AND A BACKUP-ID

  $MYSQL_BINARIES_DIR/bin/ndb_restore -c $CONNECTSTRING -m -r --nodeid=\$NODEID --backupid=\$BACKUPID

"  > $NDB_RESTORE $ECHO_OUT

chmod +x $NDB_RESTORE

}


##########################################################################################
# Function to generate script for monitoring memory usage
##########################################################################################
make_memory_usage() 
{

MEMORY_USAGE="$NDB_DIR/scripts/$MEMORY_USAGE_SCRIPT"

writeable_scripts_dir

echo "#!/bin/sh

usage()
{
   echo \"\"
   echo \"Usage: prog_name [-n NUM_LINES (to parse in cluster.log (20 default))]\"
   echo \"\"
}

NUM_LINES=20

  case \$1 in
    \"-h\"|\"--help\"|\"-help\")
      usage
      exit 0
      ;;      
    \"-n\")
      shift
      NUM_LINES=\$1
      usage
      exit 0
      ;;      
     \"\")
      # do nothing
      ;;
      *)
      echo \"Invalid switch or parameter: \$1\"
      usage
      exit 0
      ;;
  esac 

  $MYSQL_BINARIES_DIR/bin/ndb_mgm -c $CONNECTSTRING -e \"all dump 1000\"
  DATA_USAGE_STR=\`tail -\$NUM_LINES $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME | grep \"Data usage\"\`
  INDEX_USAGE_STR=\`tail -\$NUM_LINES $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME | grep \"Index usage\"\`
  RESOURCE_USAGE_STR=\`tail -\$NUM_LINES $NDB_LOGS_DIR/$CLUSTER_LOG_FILENAME | grep \"Resource\"\`
  echo \"Data Usage:\"
  echo \"\$DATA_USAGE_STR\"
  echo \"Index Usage:\"
  echo \"\$INDEX_USAGE_STR\"
  echo \"Resource Usage:\"
  echo \"\$RESOURCE_USAGE_STR\"

  exit 0
" > $MEMORY_USAGE

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not create file: $MEMORY_USAGE" $ECHO_OUT
 exit_error
fi

chmod +x $MEMORY_USAGE

if [ $? -ne 0 ] ; then
 echo "" $ECHO_OUT
 echo "Failure: could not make file executable: $MEMORY_USAGE" $ECHO_OUT
 exit_error
fi

}
 
##############################################################################
# FUNCTION TO INDENTIFY THE LINUX DISTRIBUTION
# return 1: debian/ubuntu
# return 2: fedora
# return 3: redhat/centos
# return 4: SuSE
# return 5: slackware
##############################################################################

get_linux_distribution()
{

if [ -e /etc/debian_version ] ; then
  echo "The Linux Distribution has been identified as debian/ubuntu"
  LINUX_DISTRIBUTION=1
fi

if [ -e /etc/fedora-release ] ; then
  echo "The Linux Distribution has been identified as fedora"
  LINUX_DISTRIBUTION
fi

if [ -e /etc/redhat-release ] ; then
  echo "The Linux Distribution has been identified as redhat"
  LINUX_DISTRIBUTION=3
fi

if [ -e /etc/centos-release ] ; then
  echo "The Linux Distribution has been identified as centos"
  LINUX_DISTRIBUTION=4
fi

if [ -e /etc/SuSE-release ] ; then
  echo "The Linux Distribution has been identified as SuSE"
  LINUX_DISTRIBUTION=5
fi

if [ -e /etc/slackware-version ] ; then
  echo "The Linux Distribution has been identified as slackware"
  LINUX_DISTRIBUTION=6
fi

}


##############################################################################
# HELPER FUNCTIONS TO GENERATE /etc/init.d/ndb_mgmd FILE
##############################################################################

#
# Generic function to start/stop a ndbd or a ndb_mgmd
#
start_stop_restart()
{
START_STOP_RESTART_1="
RETVAL=0

EXEC_WITH_USER=\"su $USERNAME -c\"

start() {
  echo \"Executing as '$USERNAME': \$START_PROG\"

  \$EXEC_WITH_USER \"\$START_PROG\"
  
  return \$?
}

stop() {
#  \$EXEC_WITH_USER \"\$STOP_PROG\"
  PID_FILE=\"$NDB_LOGS_DIR/ndb_$PID.pid\"
  PROCESS_ID=\`cat \$PID_FILE\`
"
START_STOP_RESTART_1a="
#  PROCESS_ID=\`expr \$PROCESS_ID + 1\`
"
START_STOP_RESTART_2="
  echo \"Shutting down\"
  kill \$PROCESS_ID

  wait_pid_removed=10
  timeout=0
  while [ \$timeout -lt \$wait_pid_removed ] ; do
    sleep 1
    test ! -s \$PID_FILE && break
    echo -n \".\"
    timeout=\`expr \$timeout + 1\`
  done
  echo \"\"

  return \$?
}

restart() {
  echo \"Executing as '$USERNAME': \$RESTART_PROG\"

  \$EXEC_WITH_USER \"\$RESTART_PROG\"

  return \$?
}


# user-supplied parameter to stop/start/restart process.
case \"\$1\" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    restart
    ;;
  reload)
    restart
    ;;
  -h|--help|-help)
    echo \"\"
    echo \"usage: <prog> start|stop|restart\"
    echo \"\"
    exit 0
    ;;
  *)
    echo $\"Usage: <prog> {start|stop|restart}\"
    exit 1
  esac


exit \$RETVAL
"

}

#
# install a ndb_mgmd or ndbd as a daemon
# $1 == process to install as a daemon
#
install_as_daemon()
{

if [ "$1" == "" ] ; then
  echo "Error: install_as_daemon must be called with a service name"
  exit 2
fi

# updates value of $LINUX_DISTRIBUTION
get_linux_distribution
echo "Linux Distro = $LINUX_DISTRIBUTION"

INSTALL_INITD=

case $LINUX_DISTRIBUTION in
   1)
	# debian/ubuntu
	INSTALL_INITD="update-rc.d ${1} defaults"
	echo ""
	echo "To remove ${1} from the auto-started services, run as admin:"
	echo "update-rc.d -f ${1} remove"
	echo ""
	;;
   2|3|4)
	# fedora/redhat/centos
	INSTALL_INITD="chkconfig --level 3 4 5 ${1} on"
	echo ""
	echo "To remove ${1} from the auto-started services, run as admin:"
	echo "chkconfig --level 3 4 5 ${1} off"
	echo ""
	;;
   5)
	# SuSE
	INSTALL_INITD="insserv -d ${1}"
	echo ""
	echo "To remove ${1} from the auto-started services, run as admin:"
	echo "insserv -r ${1}"
	echo ""
	;;
   6)
	# slackware
	echo ""
	echo "Copy ${1} to /etc/rc.d, if you want to start the service at boot-time."
	echo ""
	;;
   *)
        echo "Could not recognise the Linux Distribution, not installing service in /etc/rc.d"
	;;
   esac

echo "Installing using: $INSTALL_INITD"
${INSTALL_INITD}

 if [ $? -ne 0 ] ; then
     echo ""
     echo "Problem with installing startup scripts."
     echo ""
 fi

}


##############################################################################
# FUNCTION TO GENERATE /etc/init.d/ndb-mgmd script
# Prerequisites: $NODEID must be set correctly.
##############################################################################
make_initd_mgmd()
{

start_stop_restart

MGMD_INIT="ndb_mgmd-1"
CONFIG_INI="$NDB_DIR/config-${NUM_NODES}node.ini"
LOGGING="> $NDB_LOGS_DIR/mgmd-stdout-1.log 2>&1"
# > /dev/null"

echo "#!/bin/bash
#
# Startup script forn db_mgmd
# Author: generated by ndbinstaller.sh
#
### BEGIN INIT INFO
# Provides:                     ndb_mgmd
# Required-Start:               $local_fs $network $remote_fs
# Should-Start:                 
# Required-Stop:                $local_fs $network $remote_fs
# Default-Start:                2 3 4 5
# Default-Stop:                 0 1 6
# Short-Description:            start and stop ndb_mgmd
# Description:                  Start/Stop/Restart NDB Management Server: ndb_mgmd.
### END INIT INFO

# Variables
prog=\"$MYSQL_BINARIES_DIR/bin/ndb_mgmd\"

START_PROG=\"\$prog -f ${CONFIG_INI} $LOGGING\"
STOP_PROG=\"$MYSQL_BINARIES_DIR/bin/ndb_mgm -c ${CONNECTSTRING} -e \\\"$MGMD_ID stop\\\"\"
RESTART_PROG=\"$MYSQL_BINARIES_DIR/bin/ndb_mgm -c ${CONNECTSTRING} -e \\\"$MGMD_ID restart\\\"\"

$START_STOP_RESTART_1
$START_STOP_RESTART_2
" > /etc/init.d/${MGMD_INIT}

`chmod 755 /etc/init.d/${MGMD_INIT}`

#install_as_daemon ${MGMD_INIT}
 
}

##############################################################################
# FUNCTION TO GENERATE /etc/init.d/ndb-node-1/2/3 script
# Prerequisites: $NODEID must be set correctly.
##############################################################################
make_initd_ndbd()
{
start_stop_restart

NDBD_INIT="ndb_node-$NODEID"
LOGGING="> $NDB_LOGS_DIR/ndb_$NODEID_out.log 2>&1"
# > /dev/null"

echo "#!/bin/bash
#
# Startup script for ndbd-$NODEID
# Author: generated by ndbinstaller.sh
#
### BEGIN INIT INFO
# Provides:                     ndb_node-*
# Required-Start:               $local_fs $network $remote_fs
# Should-Start:                 
# Required-Stop:                $local_fs $network $remote_fs
# Default-Start:                2 3 4 5
# Default-Stop:                 0 1 6
# Short-Description:            start and stop ndb_node-*
# Description:                  Start/Stop/Restart NDB Data Node: ndb_node-*
### END INIT INFO

# Variables
prog=\"$NDBD_BIN\"

START_PROG=\"\$prog -c $CONNECTSTRING --ndb-nodeid=${NODEID} $LOGGING &\"
STOP_PROG=\"$MYSQL_BINARIES_DIR/bin/ndb_mgm -c ${CONNECTSTRING} -e \\\"$NODEID stop\\\"\"
RESTART_PROG=\"$MYSQL_BINARIES_DIR/bin/ndb_mgm -c ${CONNECTSTRING} -e \\\"$NODEID restart\\\"\"

$START_STOP_RESTART_1
$START_STOP_RESTART_1a
$START_STOP_RESTART_2

" > /etc/init.d/${NDBD_INIT}

`chmod 755 /etc/init.d/${NDBD_INIT}`

#install_as_daemon $NDBD_INIT

}

###################################################################################################
# MAKE init-script for MYSQLD
# COPY SCRIPTS TO $NDB_CONFIG AND /etc/init.d/mysql.server
###################################################################################################

make_initd_mysqld()
{  
    if [ "$MYSQL_NUMBER" = "" ] ; then
	MYSQL_NUMBER="1"
    fi
    MYSQLD_INIT="mysql.server"
    
    if [ $ROOTUSER = 1 ] ; then
      install_initd () 
      {
       echo "" $ECHO_OUT
       echo "'mysql.server' is a script used to start the mysql server upon machine reboot." $ECHO_OUT
       echo "" $ECHO_OUT
       echo "Install the following:" $ECHO_OUT
       echo "$MYSQL_BINARIES_DIR/support-files/mysql.server to" $ECHO_OUT
       echo "/etc/init.d/$MYSQLD_INIT ? (y/n)" $ECHO_OUT
       read ACCEPT
       case $ACCEPT in
          y | Y)
             if [ -e /etc/init.d/$MYSQLD_INIT ] ; then
    	       echo "" $ECHO_OUT
               echo "Backing up /etc/init.d/$MYSQLD_INIT to /etc/init.d/$MYSQLD_INIT.old"  $ECHO_OUT
    	       cp /etc/init.d/$MYSQLD_INIT /etc/init.d/$MYSQLD_INIT.backup
    	     fi
    	     echo "" $ECHO_OUT
    	     cp $MYSQL_BINARIES_DIR/support-files/mysql.server /etc/init.d/$MYSQLD_INIT
    	     if [ $? -ne 0 ] ; then 
    		 echo "" $ECHO_OUT
    		 echo "Failure: Could not copy mysql.server file" $ECHO_OUT
		 return 1
	     fi
	     echo "" $ECHO_OUT
#	     MY_CNF_ESCAPED=`echo "$MY_CNF" | sed -e "s/\//\\\//g"`
#	     echo "Escaped my.cnf = $MY_CNF_ESCAPED"
#	     cat /etc/init.d/$MYSQLD_INIT | sed -e "s/\/etc\/my.cnf/$MY_CNF_ESCAPED/g" > /etc/init.d/$MYSQLD_INIT.tmp
#	     mv /etc/init.d/$MYSQLD_INIT.tmp /etc/init.d/$MYSQLD_INIT
    	     echo "Info: You can start/stop/restart the mysql server using the command:" $ECHO_OUT
    	     echo "/etc/init.d/$MYSQLD_INIT start|stop|restart" $ECHO_OUT
             if [ -e /etc/my.cnf ] ; then
                 echo "" $ECHO_OUT
		 echo "Backing up /etc/my.cnf to /etc/my.cnf.backup"  $ECHO_OUT
		 cp /etc/my.cnf /etc/my.cnf.backup
             fi
             echo "" $ECHO_OUT
	     cp $MY_CNF /etc/my.cnf
	     if [ $? -ne 0 ] ; then 
		 echo "Failure: Could not copy to my.cnf file" $ECHO_OUT
	     else
		 echo "The new my.cnf file is now installed as the default mysqld configuration file." $ECHO_OUT
	     fi
            ;;
          n | N)
            ;;
          q | Q)
            exit_error
            ;;
          *)
            echo "" $ECHO_OUT
            echo "Please enter 'y', 'n', or 'q'." $ECHO_OUT
            install_initd
            ;;
        esac
      }
      install_initd
    
# If this is a 2-step installation process (one for $MYSQLD_INIT and one for my.cnf)
      install_my_cnf () 
      {
       echo "" $ECHO_OUT
       echo "Install the following:" $ECHO_OUT
       echo "$MY_CNF" $ECHO_OUT
       printf 'to /etc/my.cnf ? \(y/n/q\) '
       read ACCEPT
        case $ACCEPT in
          y | Y)
             if [ -e /etc/my.cnf ] ; then
               echo "" $ECHO_OUT
               echo "Backing up /etc/my.cn to /etc/my.cnf.backup"  $ECHO_OUT
               cp /etc/my.cnf /etc/my.cnf.backup
             fi
             echo "" $ECHO_OUT
	     cp $MY_CNF /etc/my.cnf
	     if [ $? -ne 0 ] ; then 
		 echo "Failure: Could not copy to my.cnf file" $ECHO_OUT
	     else
		 echo "The new my.cnf file is now installed as the default mysqld configuration file." $ECHO_OUT
	     fi
            ;;
          n | N)
            ;;
          q | Q)
            exit_error
            ;;
          *)
            echo "" $ECHO_OUT
            echo "Please enter 'y', 'n', or 'q'." $ECHO_OUT
            install_my_cnf
            ;;
        esac
      }
#      install_my_cnf
    

     clear_screen
    
    fi
}





###################################################################################################
###################################################################################################
###################################################################################################
# 
# START OF SCRIPT MAINLINE
# 
###################################################################################################
###################################################################################################
###################################################################################################



while [ $# -gt 0 ]; do    # Until you run out of parameters . . .
  case "$1" in
    -a|--arch)
	      shift
	      case "$1" in
		 amd64|AMD64)
		      CPU="x86_64"
		      ;;
		 # x86_64)
		 #     CPU="x86_64" 
		 #     ;;
		 # ia64|IA64)
		 #     CPU="ia64"
		 #     ;;
		 #powerpc|POWERPC)
		 #     CPU="ppc-max"
		 #     ;;
		 mac|MAC)
		      PLATFORM="mac"
		      CPU="darwin-mwcc"
		      ;;
              esac
	      ;;
    -my|--add-mysql)
	      ADD_MYSQLD=1
	      ;;
    -nc|--num-cores)
	      shift
	      NUM_CORES=$1
	      ;;
    -h|--help|-help)
	      echo "" $ECHO_OUT
	      echo "You can install MySQL Cluster as 'root' or normal user." $ECHO_OUT
	      echo "Install as 'root' to install to system directories [/usr/local/mysql/]." $ECHO_OUT
	      echo "Install as normal user to install to user directories [~/.mysql]." $ECHO_OUT
	      echo "" $ECHO_OUT
#	      echo "You can create a log of the installation using the debug (-d) option." $ECHO_OUT
#	      echo "" $ECHO_OUT
#              echo "usage: [sudo] $SCRIPTNAME [ -d|--debug]" $ECHO_OUT
              echo "usage: [sudo] ./$SCRIPTNAME "
	      echo " [-a|--arch amd64|powerpc]"
	      echo "                  select processor architecture"
	      echo " [-c|--default-config]"
	      echo "                  generate a config.ini file with default parameters"
	      echo " [-cl|--clean]    clean the install directory when installing"
	      echo " [-d|--ndbd-hostname hostname]"
	      echo "                  ndbd hostname with node-id generated using order" $ECHO_OUT
	      echo "                  in which they are specified in command line, e.g.,"
	      echo "                  -d myhost-1 -d myhost-2 have node-id 1 and 2 respectively"
	      echo " [-dm|--data-memory)]"
	      echo "                  size of data memory"
	      echo " [-h|--help]      help for ndbinstaller.sh" $ECHO_OUT
	      echo " [-i|--install-action localhost|local-mysql|mgm|ndbd|remote-mysql] " $ECHO_OUT
	      echo "                 'localhost' installs a localhost cluster"
	      echo "                 'mgm' installs a Mgm Server for a dist cluster"
	      echo "                 'ndbd' installs a Data Node for a dist cluster"
	      echo "                 'local-mysql' installs a mysqld on same host as MgmServer"
	      echo "                 'remote-mysql' installs a mysqld on different host to MgmServer"
	      echo "                  You need to have localhost set to '127.0.0.1' in /etc/hosts "
	      echo " [-id ID]         node-id for ndbd during install-action 'ndbd'" $ECHO_OUT
	      echo " [-im|--index-memory)]"
	      echo "                  size of index memory"
	      echo " [-k|--keygen]    force generation of a new SSH public key during install" $ECHO_OUT
	      echo " [-sk|--skip-mgm-pk]"
	      echo "                  Skip downloading the mgm server's public key" 
	      echo " [-m|--ndb_mgmd HOSTNAME] "
	      echo "                  set the hostname for the NDB Management Server"
	      echo " [-md|--default-mysql-dir]"
	      echo "                  accept the default installation directory for MySQL Server:"
	      echo "                  user-level default dir: ~/.mysql/ndb/mysql_[NUM]"
	      echo "                  root default dir: /var/lib/mysql-cluster/ndb/mysql_[NUM]"
	      echo " [-mh|--mysql-host HOSTNAME] "
	      echo "                  set the hostname for the MySQL Server"
	      echo " [-ms|--default-mysql-settings]"
	      echo "                  accept the default settings (in my.cnf) for the MySQL Server"
	      echo " [-my|-add-mysql]     when installing a Mgm Server, also install a MySQL Server"
	      echo " [-n|--num-nodes NUMBER_NODES]"
	      echo "                  help for ndbinstaller.sh" $ECHO_OUT
	      echo " [-nd|--default-ndb-dir]"
	      echo "                  accept the default installation directory for NDB:"
	      echo "                  user-level default dir: ~/.mysql/ndb"
	      echo "                  root installation default dir: /var/lib/mysql-cluster/ndb"
	      echo " [-ni|--non-interactive)]"
	      echo "                  skip license/terms acceptance and all confirmation screens."
              echo " [-p|--ndb_mgmd-port PORT_NUM] "
	      echo "                  set the port number for the NDB Management Server"
	      echo "                  (default $DEFAULT_MGM_PORT)"
	      echo " [--proxy URL]"
	      echo "                  set the Http Proxy address for downloading MySQL Binaries "
              echo " [--proxy-user]"
              echo "                  set the Http Proxy username or downloading MySQL Binaries "
              echo " [--proxy-pass]"
	      echo "                  set the Http Proxy password for downloading MySQL Binaries "
	      echo " [-u|--username USERNAME]"
	      echo "                  set the username for install of MgmServer/DataNode/MySQL "
	      echo " [-v|--version]   version information for ndbinstaller.sh" 
	      echo "" 
	      exit 3
              break ;;

    -i|--install-action)
	      shift
	      case $1 in
		 localhost)
		      INSTALL_ACTION=$INSTALL_LOCALHOST
		      ;;
		 local-mysql)
		      INSTALL_ACTION=$INSTALL_LOCALHOST_MYSQLD
		      ;;
	         mgm)
		      INSTALL_ACTION=$INSTALL_MGM
		      ;;
		 ndbd)
		      INSTALL_ACTION=$INSTALL_NDB
		      ;;
		 remote-mysql)
		      INSTALL_ACTION=$INSTALL_MYSQLD
		      ;;
		  *)
		      echo "Could not recognise option: $1"
		      exit_error "Failed."
		 esac
	       ;;
    -id)
	      shift
	      NODEID=$1
	      ;;
    -k|--keygen)  # generate a ssh-key, even if one already exists
	      FORCE_GENERATE_SSH_KEY=1
	      ;;
    -sk|--skip-mgm-pk)
	      SKIP_DOWNLOAD_MGM_SSH_KEY=1
	      ;;
    --debug) # enable debugging
              DEBUG=1
	      ;;
    -mh|--mysql-host)
	      shift
	      MYSQL_HOST=$1
	      MYSQL_HOST_NOT_SET=0
	      ;;
    -m|--ndb_mgmd)
	      shift
	      MGM_HOST=$1
	      MGM_HOST_NOT_SET=0
	      ;;
    -p|--ndb_mgmd-port)
	      shift
	      MGM_PORT=$1
	      MGM_PORT_NOT_SET=0
	      ;;
    -n|--num-nodes) 
	      shift
	      case $1 in
	      1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16)
		      PARAM_NUM_NODES=$1
		      ;;
		  *) exit_error "Invalid number of nodes. Must be a value between 1-16."
		      ;;
	      esac
	      ;;
    -d|--ndbd-hostname)
	      shift
	      NDB_HOST[$NDBD_PARAM_NO]="$1"
	      NDBD_PARAM_NO=`expr $NDBD_PARAM_NO + 1`
	      ;;
    -c|--default-config)
	      PARAM_CONFIG_DEFAULT=1
	      ;;
    -cl|--clean)
	      CLEAN_INSTALL_DIR=1
	      ;;
    -nd|--default-ndb-dir)
	      PARAM_DEFAULT_INSTALL_NDB_DIR=1
	      ;;
    -md|--default-mysql-dir)
	      PARAM_DEFAULT_INSTALL_MYSQL_DIR=1
	      ;;
    -ms|--default-mysql-settings)
	      PARAM_DEFAULT_MYSQL_SETTINGS=1
	      ;;
    -u|--username)
	      shift
	      PARAM_USERNAME=1
	      USERNAME=$1
	      ;;
    -dm|--data-memory)
	      shift
	      DATA_MEMORY=$1
	      ;;
    -im|--index-memory)
	      shift
	      INDEX_MEMORY=$1
	      ;;
    -ni|--non-interactive)
	      NON_INTERACT=1
	      ;;
    --proxy)
	      shift
	      HTTP_PROXY=$1
	      ;;
    --proxy-user)
	      shift
	      HTTP_USER=$1
	      ;;
    --proxy-pass)
	      shift
	      HTTP_PASS=$1
	      ;;
    -v|--version) 
	      echo "" $ECHO_OUT    
              echo -e $NDB_INSTALLER_VERSION $ECHO_OUT
	      echo "" $ECHO_OUT
              echo "MySQL Cluster Version: mysql-${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}${RELEASE}" $ECHO_OUT
	      echo "" $ECHO_OUT
              exit 0
              break ;;
    *)
	  exit_error "Unrecognized parameter: $1"
	  ;;
  esac
  shift       # Check next set of parameters.
done

if [ $NDBD_PARAM_NO -gt 0 ] ; then
    if [ "$PARAM_NUM_NODES" = "" ] ; then
	PARAM_NUM_NODES=$NDBD_PARAM_NO
    fi
fi

##############################################################################################
# FINISH CONFIGURING VARIABLES
##############################################################################################

configure_vars()
{

  if [ "$STOP_NDB" = "1" ] ; then
    echo "Executing $NDB_DIR/scripts/$CLUSTER_SHUTDOWN $CONNECTSTRING $MYSQL_CONNECT" $ECHO_OUT
   $MYSQL_BINARIES_DIR/scripts/$CLUSTER_SHUTDOWN $CONNECTSTRING $MYSQL_CONNECT
  fi
  
  
  # Set connectstring for mgmd
  CONNECTSTRING=${MGM_HOST}:${MGM_PORT}
  
  # Set the connect string for the mysqld
  MYSQL_CONNECT=${MYSQL_HOST}:${MYSQL_PORT}
  
  # Log for installer (current directory not writeable?)
  if [ ! -w $CWD ]  ; then
      INSTALL_LOG="/tmp/"$INSTALL_LOG
  fi
  
  ECHO_OUT=
  # If Debugging is enabled, write output to installer log, not stdout
  if [ "$DEBUG" = "1" ] ; then
      ECHO_OUT=>>${INSTALL_LOG}
  fi
  
}

##############################################################################################
# HELPER FUNCTIONS
##############################################################################################

# $1 == program name

is_installed_prog()
{
    $1 --version >& /dev/null

    if [ $? -ne 0 ] ; then
        echo "" $ECHO_OUT
        echo "Failure: You do not seem to have the program '$1' installed." $ECHO_OUT
        echo "" $ECHO_OUT
        echo "Install $1 before proceeding." $ECHO_OUT
        echo "ubuntu/debian: apt-get install $1" $ECHO_OUT
        echo "" $ECHO_OUT
        exit_error
    fi
}


is_installed_lib()
{

    ldd /usr/lib/$1.so  >& /dev/null

    if [ $? -ne 0 ] ; then 
	echo "" $ECHO_OUT
	echo "Warning: You do not seem to have the library '$1-dev' installed." $ECHO_OUT
	echo "" $ECHO_OUT
        echo "Install $1-dev before proceeding." $ECHO_OUT
        echo "ubuntu/debian: apt-get install $1-dev" $ECHO_OUT
	echo "" $ECHO_OUT
        clear_screen
    fi
}


installed_progs()
{
    #PROG=make
    #is_installed_prog $PROG

    #PROG=aclocal
    #is_installed_prog $PROG

    #PROG=automake
    #is_installed_prog $PROG

    #PROG=libtool
    #is_installed_prog $PROG

    PROG=tar
    is_installed_prog $PROG

    #PROG=libtool
    #is_installed_prog $PROG

    #PROG=g++
    #is_installed_prog $PROG

    #LIB=libncurses
    #is_installed_lib $LIB

    #LIB=libaio1
    #is_installed_lib $LIB

#    PROG=flex
#    is_installed_prog $PROG

#    PROG=yacc
#    is_installed_prog $PROG
}



checkports()
{
  PORTLIST="${MYSQL_PORT} ${MGM_PORT}"
  for port in ${PORTLIST}; do
    echo "Info : Checking port ${port} is not in use"
    PORT_COUNT=`netstat -a --tcp | grep -v WAIT | grep -c ${port}`
    if [[ ${PORT_COUNT} -gt 0 ]]; then
      echo "Error: Port \"${port}\" is in use."
	  echo "       A database process is probably already running using this port."
	  echo "       Is it yours? The Database processes running are:"
	  echo
	  echo "   User  PID   Command"
	  echo "   ----  ---   -------"
	  ps -e -o user,pid,args | grep "ndb\|mysql" | grep -v grep | grep -v ${SCRIPTNAME}
          netstat -a | grep ${port}
	  echo
	  echo "Exiting..."
	  exit 1
    fi
  done
}

#
# Checks if you are running Linux, exit if not.
#
check_linux()
{
  OP_SYS=`uname`
  if [[ ${OP_SYS} != "Linux" ]]; then
    exit_error "This script only works for Linux."
  fi
}

#
# Checks if you are running the script as root or not
#
check_userid()
{
  ROOTUSER=0
  
  # Check if user is root
  USERID=`id | sed -e 's/).*//; s/^.*(//;'`
  if [ "X$USERID" = "Xroot" ]; then
    ROOTUSER=1
    MYSQL_BASE_DIR=/usr/local
  else
    echo "USER!!!!"
    HOMEDIR=`(cd ; pwd)`
    MYSQL_BASE_DIR=$HOMEDIR/.mysql
    NDB_INSTALL_DIR=$HOMEDIR/.mysql
    NDB_DIR=$NDB_INSTALL_DIR/$NDB_VERSION
    NO_DAEMON="--nodaemon"
    NO_DAEMON_LAUNCH="&"
    # over-ride the default
    TEST_USERID="" 
    USERNAME=$USERID
  fi
}


select_cpu() 
{
    if [ "$CPU" = "" ] ; then
        echo "Select the computer architecture for this host: " $ECHO_OUT
	echo "(All management nodes and data nodes must run the same version of MySQL)" $ECHO_OUT
	echo "" $ECHO_OUT
	echo "(1) X86_64" $ECHO_OUT
	echo "(2) MAC" $ECHO_OUT
	echo "" $ECHO_OUT
	printf 'Please enter your choice '1','2', or 'h' \(help\) :  '
        read ACCEPT

        case $ACCEPT in
          1)
            CPU="x86_64"
            ;;
          2)
            CPU="powerpc"
            ;;
          h | H)
	  clear
	  echo -e $CPU_HELP
	  clear_screen
          select_cpu
          ;;
          q | Q)
          exit_error
          ;;
          *)
            echo "" $ECHO_OUT
            echo "Invalid Choice: $ACCEPT." $ECHO_OUT
            echo "" $ECHO_OUT
	    clear_screen
            select_cpu
            ;;
         esac
	clear_screen
   fi
}


#
# Sets up the location of the mysql binaries
#
setup_binaries_dir()
{

  NDB_VERSION="ndb-${NDB_VERSION_MAJOR}.${NDB_VERSION_MINOR}.${NDB_VERSION_REV}${RELEASE}"
#  VERSION="mysqlcom-${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}-${NDB_VERSION}${RELEASE}"
#  VERSION="mysql-${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}-${NDB_VERSION}"
  #VERSION="${VERSION_PREFIX}${NDB_VERSION_MAJOR}.${NDB_VERSION_MINOR}.${NDB_VERSION_REV}"
#VERSION="${VERSION_PREFIX}${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}-${PLATFORM}-${CPU}-glibc23"
VERSION="${VERSION_PREFIX}${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_REV}-${PLATFORM}-${CPU}"

  MYSQL_INSTALLED=".mysql_installed_"${VERSION}
  MYSQL_BINARIES_INSTALLED=".mysql_binaries_installed_"${VERSION}

  MYSQL_BINARIES_DIR=$MYSQL_BASE_DIR/$VERSION
  NDBD_BIN=$MYSQL_BINARIES_DIR/bin/$NDBD_PROG
  NDB_DIR=$NDB_INSTALL_DIR/$NDB_VERSION
}

#######################################################################
# Introduction Splash Screen
#######################################################################

splash_screen() 
{
  clear
  echo "" $ECHO_OUT
  echo "NdbInstaller, Copyright(C) 2007 Jim Dowling. All rights reserved." $ECHO_OUT
  echo "" $ECHO_OUT
  echo "This program installs MySQL Server and NDB Storage Engine (MySQL Cluster)," $ECHO_OUT
  echo "Copyright(C) MySQL AB."  $ECHO_OUT
  echo "" $ECHO_OUT
  #echo "REQUIREMENTS: openssh-server, libncurses-dev, automake, autoconf, libtool, make, gcc, g++" $ECHO_OUT
  #echo "Installation (ubuntu/debian): apt-get install openssh-server build-essential automake autoconf gcc g++ libtool libncurses5-dev" $ECHO_OUT
  #echo "" $ECHO_OUT
  if [ $ROOTUSER -eq 1 ] ; then
    echo "You are running the Installer as a root user." $ECHO_OUT
    echo "This will install MySQL and NDB by default to: /var/lib/ndb/" $ECHO_OUT
  else
    echo "You are running the Installer as a non-root user." $ECHO_OUT
    echo "This will install MySQL and NDB by default to: ~/.mysql/" $ECHO_OUT
    echo "To install cluster in /var/lib/ndb/, re-run this installer as 'root'." $ECHO_OUT
  fi
  echo ""  $ECHO_OUT
  echo "To cancel installation at any time, press CONTROL-C"  $ECHO_OUT
  
  clear_screen
}

#######################################################################
# LICENSING
#######################################################################

display_license()
{
  echo "Support is available at http://www.jiimdowling.info/ndbinstaller-trac/" $ECHO_OUT
  echo ""  $ECHO_OUT
  echo "This code is released under the GNU General Public License, Version 3, see:" $ECHO_OUT
  echo "http://www.gnu.org/licenses/gpl-3.0.txt" $ECHO_OUT
  echo "" $ECHO_OUT
  echo "Copyright(C) 2007 Jim Dowling. All rights reserved." $ECHO_OUT
  echo "Jim Dowling is furnishing this item "as is". Jim Dowling does not provide any" $ECHO_OUT
  echo "warranty of the item whatsoever, whether express, implied, or statutory," $ECHO_OUT
  echo "including, but not limited to, any warranty of merchantability or fitness" $ECHO_OUT
  echo "for a particular purpose or any warranty that the contents of the item will" $ECHO_OUT
  echo "be error-free. In no respect shall Jim Dowling incur any liability for any" $ECHO_OUT 
  echo "damages, including, but limited to, direct, indirect, special, or consequential" $ECHO_OUT
  echo "damages arising out of, resulting from, or any way connected to the use of the" $ECHO_OUT
  echo "item, whether or not based upon warranty, contract, tort, or otherwise; " $ECHO_OUT 
  echo "whether or not injury was sustained by persons or property or otherwise;" $ECHO_OUT
  echo "and whether or not loss was sustained from, or arose out of, the results of," $ECHO_OUT
  echo "the item, or any services that may be provided by Jim Dowling." $ECHO_OUT
  echo "" $ECHO_OUT
  printf 'Do you accept these terms and conditions? [ yes or no ] '
}
  
accept_license () 
  {
    read ACCEPT
    case $ACCEPT in
      yes | Yes | YES)
        ;;
	no | No | NO)
        echo "" $ECHO_OUT
        exit 0
        ;;
      *)
        echo "" $ECHO_OUT
        echo "Please enter either 'yes' or 'no'." $ECHO_OUT
	printf 'Do you accept these terms and conditions? [ yes or no ] '
        accept_license
      ;;
     esac
}



#######################################################################
# INSTALL OPTIONS
#######################################################################

install_action() {
    if [ "$INSTALL_ACTION" = "" ] ; then

        echo "-------------------- Installation Options --------------------" $ECHO_OUT
	echo "" $ECHO_OUT
        echo "What would you like to do?" $ECHO_OUT
	echo "" $ECHO_OUT
#	echo "(0) Download and build sources for MySQL Cluster telco." $ECHO_OUT
#	echo "" $ECHO_OUT
	echo "(1) Setup a localhost cluster." $ECHO_OUT
	echo "" $ECHO_OUT
	echo "(2) Add a MySQL server to an installed localhost cluster." $ECHO_OUT
	echo "" $ECHO_OUT
	echo "" $ECHO_OUT
	echo "(3) Setup a management server for a distributed (non-localhost) cluster." $ECHO_OUT
	echo "" $ECHO_OUT
	echo "(4) Add a data node to a distributed cluster." $ECHO_OUT
	echo "" $ECHO_OUT
	echo "(5) Add a MySQL server to an installed distributed cluster." $ECHO_OUT
	echo "" $ECHO_OUT
	printf 'Please enter your choice '1', '2', '3', '4', '5', 'q' \(quit\), or 'h' \(help\) :  '
        read ACCEPT
        case $ACCEPT in
#          0)
#	    INSTALL_ACTION=$INSTALL_COMPILE
#            ;;
          1)
            if [ $ROOTUSER -eq 1 ] ; then
	       echo "" $ECHO_OUT
	       echo "You cannot install a localhost cluster as 'root'." $ECHO_OUT
	       exit_error "Run the installer again as a normal user to install a localhost cluster."
	    fi

	    INSTALL_ACTION=$INSTALL_LOCALHOST
            ;;
          2)
	    INSTALL_ACTION=$INSTALL_LOCALHOST_MYSQLD
            ;;
          3)
	    INSTALL_ACTION=$INSTALL_MGM
            ;;
          4)
	    INSTALL_ACTION=$INSTALL_NDB
            ;;
          5)
	    INSTALL_ACTION=$INSTALL_MYSQLD
            ;;
          h | H)
	  clear
	  get_install_option_help
	  echo -e $INSTALL_OPTION_HELP
          clear_screen_no_skipline
          install_action
          ;;
          q | Q)
          exit_error
          ;;
          *)
            echo "" $ECHO_OUT
            echo "Invalid Choice: $ACCEPT" $ECHO_OUT
            echo "Please enter your choice '1', '2', '3', '4', 'q', or 'h'." $ECHO_OUT
	    clear_screen
            install_action
            ;;
         esac
	clear_screen
   fi
}


############################################################################################
# Helper scripts for finding already installed mysql binaries and mysql data directories.
############################################################################################
find_mysql_binaries()
{
   if [ -d $MYSQL_BASE_DIR ]; then      
       # TODO: Change this to find . -type d -name "mysql-*" -regex ".*mysql-[0-9]+.*[^test]"

       MYSQLBIN=`find $MYSQL_BASE_DIR -type d -name "mysql-*" -regex ".*mysql-cluster-gpl-[0-9]+.*[^test]" -print`
       echo "bins = $MYSQLBIN"
       NUM_MYSQL_BINS=`find $MYSQL_BASE_DIR -type d -name "mysql-*" -regex ".*mysql-cluster-gpl-[0-9]+.*[^test]" -print | wc -l`
       echo "num bins = $NUM_MYSQL_BINS"
   fi
}

find_mysql_data_and_start_scripts()
{

   if [ -d $NDB_INSTALL_DIR ]; then      


      MYSQL_DATA_INSTALLATIONS=`find $NDB_INSTALL_DIR -type f -name $MYSQL_INSTALLED -print` 

#      echo "Found: $MYSQL_DATA_INSTALLATIONS"

      NUM_MYSQLS=`find $NDB_INSTALL_DIR -type f -name $MYSQL_INSTALLED -print | wc -l` 

#      echo "Number of MySQLs: $NUM_MYSQLS"
      if [ $? -ne 0 ] ; then
	  echo "Problem with finding startup scripts. Patching scripts for mysqld installation may not work."
      fi
      START_SCRIPTS=`find $NDB_INSTALL_DIR -type f -name 'start-noinit-cluster*.sh' -print`
      if [ $? -ne 0 ] ; then
	  echo "Problem with finding startup scripts. Patching scripts for mysqld installation may not work."
      fi
      INIT_SCRIPTS=`find $NDB_INSTALL_DIR -type f -name 'init-start-cluster*.sh' -print` 
      if [ $? -ne 0 ] ; then
	  echo "Problem with finding startup scripts. Patching scripts for mysqld installation may not work."
      fi
      SHUTDOWN_SCRIPTS=`find $NDB_INSTALL_DIR -type f -name 'shutdown-cluster.sh' -print` 
      if [ $? -ne 0 ] ; then
	  echo "Problem with finding startup scripts. Patching scripts for mysqld installation may not work."
      fi
   fi
    START_SCRIPTS="$START_SCRIPTS $INIT_SCRIPTS" 
}

select_mysql_installation() 
{
 echo "" $ECHO_OUT
 count=0
 for install in "$@" ; do
#    install=`dirname $install`
   if [ -d $install"/bin" -a -f $install"/bin/mysql" ]; then  
      count=`expr $count + 1`
      echo "$count) $install" $ECHO_OUT
   fi
 done
 
 pick_one()
 {
   echo "Pick one of the above installations (1-"$count")" $ECHO_OUT
   read install_num
   case $install_num in
          0|1|2|3|4|5|6|7|8|9)
            ;;
          *)
            echo "" $ECHO_OUT
            echo "Invalid Choice: $ACCEPT." $ECHO_OUT
            echo "" $ECHO_OUT
	    pick_one
            ;;
         esac
 }
 pick_one
      
 count=0
 for install in "$@" ;  do

#   if [ -d "$install/bin" -a -f "$install/bin/mysql" ]; then  
       count=`expr $count + 1`
#   fi
   if [ "$install_num" = "$count" ] ; then
#       MYSQL_BINARIES_DIR=`dirname $install`
       MYSQL_BINARIES_DIR="$install"
       NDBD_BIN=$MYSQL_BINARIES_DIR/bin/$NDBD_PROG
   fi
 done
     
 NDBD_BIN=$MYSQL_BINARIES_DIR/bin/$NDBD_PROG
 echo "binary dir = $MYSQL_BINARIES_DIR"
}

check_for_mysql_datadirs()
{
 echo "-------------- Looking for existing MySQL Data Directories --------------" $ECHO_OUT 
 echo "" $ECHO_OUT
 echo "Executing 'find' command for '$MYSQL_INSTALLED' file in directory:" $ECHO_OUT
 echo "$NDB_INSTALL_DIR" $ECHO_OUT
 echo "" $ECHO_OUT
 echo "This can take a few moments. Please wait......" $ECHO_OUT

  find_mysql_data_and_start_scripts
  clear
  COUNT=1
  PORT_INC=0

      echo "-------------- Setup MySQL Data Directory --------------" $ECHO_OUT 

  if [ $NUM_MYSQLS -gt 0 ] ; then
      echo "" $ECHO_OUT
      echo "List of installed MySQL Data Directories:" $ECHO_OUT
      echo "$MYSQL_DATA_INSTALLATIONS" $ECHO_OUT
      echo "" $ECHO_OUT


        if [ "$INSTALL_ACTION" = "$INSTALL_LOCALHOST_MYSQLD" ] ; then
	    INSTALL_ACTION=$INSTALL_ANOTHER_MYSQLD_LOCALHOST
	elif [ "$INSTALL_ACTION" = "$INSTALL_MYSQLD" ] ; then
	    INSTALL_ACTION=$INSTALL_ANOTHER_MYSQLD	
	else
	    echo "Invalid install action $INSTALL_ACTION"
	    exit -9
        fi

        get_ndb_dir
        #0. Ask if full-install or just datadir
        #1. Get datadir
        #2. Check for valid directory
  
        MYSQL_INSTALL_DIR=${NDB_DIR}"/mysql_"
        FINISHED=0
        while [ $FINISHED -eq 0 ] ; do
          COUNT=`expr $COUNT + 1`
          if [ ! -d "${MYSQL_INSTALL_DIR}${COUNT}" ] ; then
  	    FINISHED=1;
	  fi
        done
        MYSQL_INSTALL_DIR=${MYSQL_INSTALL_DIR}${COUNT}
	MYSQL_SOCK="$MYSQL_INSTALL_DIR""/mysql.sock"
#      fi
  else

      if [ ! -d $NDB_DIR ] ; then
	  setup_ndb_dirs
	  NDB_LOGS_DIR=${NDB_INSTALL_DIR}/logs
	  echo "-------------- Setup MySQL Data Directory --------------" $ECHO_OUT
      fi

     MYSQL_INSTALL_DIR=${NDB_DIR}"/mysql_1"
     echo "mysql dir is: ${MYSQL_INSTALL_DIR}" 
  fi

  if [ ! -d $NDB_DIR ] ; then
      setup_ndb_dirs
      echo "-------------- Setup MySQL Data Directory --------------" $ECHO_OUT
  fi

  NDB_LOGS_DIR=${NDB_INSTALL_DIR}/logs

  PORT_INC=`expr $COUNT - 1`
  MYSQL_PORT=`expr $MYSQL_PORT + $PORT_INC`
  HELP_STR="DataDir"       
  if [ $ROOTUSER -eq 1 ] ; then
     MYSQL_DATA_DIR="${MYSQL_INSTALL_DIR}/data"
     install_dir $MYSQL_DATA_DIR $MYSQL_INSTALL_DIR $HELP_STR "data" $PARAM_DEFAULT_INSTALL_MYSQL_DIR
  else
     MYSQL_DATA_DIR="${MYSQL_INSTALL_DIR}/var"
     install_dir $MYSQL_DATA_DIR $MYSQL_INSTALL_DIR $HELP_STR "var" $PARAM_DEFAULT_INSTALL_MYSQL_DIR
  fi

  if [ $FINISHED_INSTALLDIRS -eq 2 ] ; then
    MOVE_BINARIES=1
  fi
  clear_screen
}


check_for_mysql_binaries()
{
 echo "-------------- Searching for Existing Binaries --------------" $ECHO_OUT 
 echo "" $ECHO_OUT
 echo "Executing 'find' command for $MYSQL_BINARIES_INSTALLED file in directory:" $ECHO_OUT
 echo "$MYSQL_BASE_DIR" $ECHO_OUT
 echo "" $ECHO_OUT
 echo "This can take a few moments. Please wait......" $ECHO_OUT

 find_mysql_binaries
 clear

  if [ $NUM_MYSQL_BINS -gt 0 ] ; then 
      echo "-------------- Use Existing MySQL Binaries ? --------------" $ECHO_OUT 
      echo "" $ECHO_OUT
      echo "Existing mysql installations:" $ECHO_OUT
      for install in $MYSQLBIN ; do
#        install=`dirname $install`
        if [ -d $install"/bin" -a -f $install"/bin/mysql" ]; then  
               echo "$install" $ECHO_OUT
        fi
      done

      echo "" $ECHO_OUT
      echo -n "Use existing binaries for this installation?" $ECHO_OUT
      if [ $NON_INTERACT -eq 0 ] ; then
         entry_ok $USE_BINARIES_HELP
      else 
         eval true  # Zero exit status to download new binaries
      fi
 
 
      if [ $? -eq 1 ] ; then
  	INSTALL_BINARIES=0

        if [ $NUM_MYSQL_BINS -gt 1 ] ; then
           select_mysql_installation $MYSQLBIN
        elif [ $NUM_MYSQL_BINS -eq 1 ] ; then
         # There may be a stray ".mysql_installed" file lying around, 
         # so check that the first directory is an installation
          for install in $MYSQLBIN ; do
#            install=`dirname $install`
            if [ -d "$install/bin" -a -f "$install/bin/mysql" ]; then  
              MYSQL_BINARIES_DIR=$install
            fi
          done
        else 
  	  echo "" $ECHO_OUT
  	  echo "Unexpected number of mysql binary installations: $NUM_MYSQL_BINS" $ECHO_OUT
  	  exit_error
        fi
      fi  
     NDBD_BIN="$MYSQL_BINARIES_DIR/bin/$NDBD_PROG"

     NDBD_BIN=$MYSQL_BINARIES_DIR/bin/$NDBD_PROG
     clear_screen
  fi 
}


#######################################################################
# EXTRACTING MYSQL BINARIES ARCHIVE
#######################################################################

install_binaries()
{ 
   if [ ! -d $VERSION ] ; then
    cd $CWD
  
    if [ ! -e $VERSION.tar.gz ] ; then
      echo "-------------- Download Binaries for MySQL --------------" $ECHO_OUT
      echo "" $ECHO_OUT
      echo "Cannot find the MySQL binaries in the current directory." $ECHO_OUT
      echo "$CWD/$VERSION.tar.gz file not found" $ECHO_OUT
      echo "" $ECHO_OUT

      if [ $NON_INTERACT -eq 0 ] ; then 
          echo "Will now try to use 'wget' program to download the MySQL binaries." $ECHO_OUT
          echo -n "Do you need to go through a web proxy to access the Internet?" $ECHO_OUT
          entry_ok

	  if [ $? -eq 1 ] ; then 
	      HTTP_PROXY=$http_proxy
	      if [ "$HTTP_PROXY" = "" ] ; then
		  echo "" $ECHO_OUT
		  echo "You need to set the environment variable 'http_proxy' to contain" $ECHO_OUT
		  echo "the URL of the proxy server for 'wget' to download the binaries." $ECHO_OUT
		  echo "" $ECHO_OUT
		  echo "Set the 'http_proxy' env variable and run this installer script again:" $ECHO_OUT
		  echo "bash>export http_proxy=myproxyserver:port" $ECHO_OUT
		  echo "csh>setenv http_proxy myproxyserver:port" $ECHO_OUT
		  exit_error
	      fi
	      HTTP_USER=$http_user
	      if [ "$HTTP_USER" = "" ] ; then
		  echo "" $ECHO_OUT
		  echo "Enter the username for the proxy server:" $ECHO_OUT
		  read ACCEPT
		  HTTP_USER=$ACCEPT
              fi
	      HTTP_PASS=$http_pass
	      if [ "$HTTP_PASS" = "" ] ; then
		  echo "" $ECHO_OUT
		  echo "Enter the password for the proxy server:" $ECHO_OUT
		  read ACCEPT
		  HTTP_PASS=$ACCEPT
              fi
	      PROXY="--proxy $HTTP_PROXY"
	  fi
      fi
      echo ""

      download_binaries

      clear_screen

     fi
     echo "-------------- Information Only --------------"   $ECHO_OUT
     echo "" $ECHO_OUT
     echo "Wait: Extracting. This step may take a few moments... " $ECHO_OUT
     echo "$VERSION.tar.gz to" $ECHO_OUT
     echo "$CWD/$VERSION" $ECHO_OUT  

     if [ ! -w $CWD ] ; then
        echo "" $ECHO_OUT
        echo "Cannot extract archive" $ECHO_OUT
        echo "Directory is not writeable: $CWD " $ECHO_OUT
	exit_error
     fi 
     tar zxf $VERSION.tar.gz
  
     if [ $? -ne 0 ] ; then
       echo "Couldn't extract $VERSION.tar.gz"        $ECHO_OUT
       echo "" $ECHO_OUT
       echo "Exiting..." $ECHO_OUT
       exit_error
     fi
     echo "" $ECHO_OUT
     echo "$VERSION.tar.gz extracted in:" $ECHO_OUT
     echo "$CWD" $ECHO_OUT
  
  else
    echo "-------------- Information Only --------------" $ECHO_OUT
    echo ""  $ECHO_OUT
    echo "Binaries to be unzipped found in directory:" $ECHO_OUT
    echo "$CWD/$VERSION" $ECHO_OUT
    fi
  
  clear_screen
  
}  


#######################################################################
# SETUP MYSQL AND NDB INSTALL PATHS
#######################################################################

get_ndb_dir()
{
  if [ "${NDB_HOME}" = "" ] ; then

    if [ ! -d $NDB_DIR ] ; then
	clear_screen
	setup_ndb_dirs
    else
      clear_screen
      echo "-------- Enter the path for the NDB Data Directory ---------" $ECHO_OUT
      echo -e "\n\$NDB_HOME is the installation directory for NDB." $ECHO_OUT
      echo -e "\n\$NDB_HOME has been found here: " $ECHO_OUT
      echo "$NDB_DIR" $ECHO_OUT
      echo "" $ECHO_OUT
      echo -n "Is the above directory the correct directory for \$NDB_HOME?" $ECHO_OUT
      if [ $NON_INTERACT -eq 0 ] ; then
         entry_ok "\$NDB_HOME is the directory where you installed the existing MySQL Data directory." 
      else 
         eval false
      fi
      if [ $? -eq 0 ] ; then
  	echo "Enter the directory where NDB is installed \(\$NDB_HOME\): " $ECHO_OUT
  	read dir
  	if [ ! -d $dir ] ; then
  	    exit_error "Directory not found: $dir"
  	fi
  	NDB_DIR=$dir
  	NDB_INSTALL_DIR=${NDB_DIR}/..
      fi
   fi
 fi
  NDB_LOGS_DIR=${NDB_INSTALL_DIR}/logs
}

setup_mysql_binary_dir()
{

    echo "----------- Setting up MySQL Server Binaries Directory  -----------" $ECHO_OUT
    HELP_STR="MySQL"
    echo "" $ECHO_OUT 
    echo "In a subsequent step, you will be prompted to create a symbolic link from:" $ECHO_OUT
    echo "$MYSQL_BINARIES_DIR" $ECHO_OUT 
    echo "to $MYSQL_BASE_DIR/mysql" $ECHO_OUT  
    install_dir $MYSQL_BINARIES_DIR $MYSQL_BASE_DIR $HELP_STR $VERSION $PARAM_DEFAULT_INSTALL_MYSQL_DIR
    if [ $FINISHED_INSTALLDIRS -eq 2 ] ; then
	MOVE_BINARIES=1
    fi    
    clear_screen
}

setup_ndb_dirs()
{
    echo "-------------- Setting up NDB Storage Engine Directory  --------------" $ECHO_OUT

    HELP_STR="NDB"
    echo "" $ECHO_OUT 
    echo "In a subsequent step, you will be prompted to create a symbolic link from:" $ECHO_OUT
    echo "$NDB_DIR" $ECHO_OUT 
    echo "to $NDB_INSTALL_DIR/ndb" $ECHO_OUT  

    install_dir $NDB_DIR $NDB_INSTALL_DIR $HELP_STR $NDB_VERSION $PARAM_DEFAULT_INSTALL_NDB_DIR

    if [ ! -d $NDB_DIR/scripts ] ; then
        mkdir $NDB_DIR/scripts
	if [ $? -ne 0 ] ; then
	    mkdir_error $NDB_DIR/scripts
	fi
    fi

    NDB_LOGS_DIR=${NDB_INSTALL_DIR}/logs

    if [ ! -d $NDB_LOGS_DIR ] ; then
      mkdir $NDB_LOGS_DIR
      if [ $? -ne 0 ] ; then
        mkdir_error $NDB_LOGS_DIR
      fi
    fi


    clear_screen
}

setup_mycnf_name()
{
     MY_CNF=$MYSQL_INSTALL_DIR/my-$MYSQL_VERSION_MAJOR-$MYSQL_VERSION_MINOR-$MYSQL_VERSION_REV.cnf
}

  
#######################################################################
# INSTALL MYSQL BINARIES TO CHOSEN PATH
#######################################################################

build_and_move_binaries()
{
  echo "--------- Moving MySQL Binaries to Installation Directory ---------" $ECHO_OUT
  echo "" $ECHO_OUT  
  #echo "Compiling MySQL Cluster binaries." $ECHO_OUT
  #echo "" $ECHO_OUT  
  #echo "Compilation will take a few minutes \(10-60 minutes\). Be patient......" $ECHO_OUT
  cd $CWD


 if [ $MOVE_BINARIES -eq 1 ] ; then

	 #echo "" $ECHO_OUT
	 #echo "The downloaded source files will now be built using autotools and automake." $ECHO_OUT
	 #echo "" $ECHO_OUT
#	 cd $VERSION
#	 if [ $? -ne 0 ] ; then
	     #echo "" $ECHO_OUT
	     #echo "There appeared to be a problem when changing directory to the sources just downloaded." $ECHO_OUT
	     #exit_error
	 #fi
	 #clear_screen
	 #BUILD/compile-${CPU} --prefix=$MYSQL_BINARIES_DIR

#         ./configure --with-plugins=ndbcluster,clusterj,openjpa  --with-extra-charsets=all --with-ssl --prefix=$MYSQL_BINARIES_DIR --with-unix-socket-path=$MYSQL_SOCK --with-ndb-docs 
#         make

	 #if [ $? -ne 0 ] ; then
	 #    cd ..
	 #    rm -rf $VERSION
	 #    echo "" $ECHO_OUT
	 #    echo "" $ECHO_OUT
	 #    echo "There appeared to be a problem when  running 'make' on the MySQL Cluster binaries." $ECHO_OUT
	 #    echo ""
	 #    echo "To build MySQL Cluster, you need to have the following programs installed:" $ECHO_OUT
	     #echo "automake, autoconf, make, gcc, g++, libtool" $ECHO_OUT
	     #echo "For debian/ubuntu run: "
	     #echo "  apt-get install build-essential automake autoconf g++ libtool flex bison" $ECHO_OUT
	     #echo "" $ECHO_OUT
	     #exit_error
         #fi
  
	 #make install
	 #if [ $? -ne 0 ] ; then
	 #    echo "" $ECHO_OUT
	 #    echo "There appeared to be a problem when running 'make install' on the MySQL Cluster binaries." $ECHO_OUT
	     #echo "" $ECHO_OUT
	     #echo "You should exit now using ctrl-c." $ECHO_OUT
	     #echo "If you continue your installation, your version of cluster may not work correctly." $ECHO_OUT	     
         #fi
	 #cd ..

	 #if [ $REMOVE_EXPANDED_BINARIES_AFTER_INSTALL -eq 1 ] ; then
	     #rm -rf $VERSION
	     #if [ $? -ne 0 ] ; then
	#	 exit_error
	     #fi
	 #fi

    #create a file that is used by subsequent runs to find existing installations
         test -e $MYSQL_BINARIES_DIR"/${MYSQL_BINARIES_INSTALLED}" 
         if [ $? -ne 0 ] ; then
           `cp -r ${VERSION}/* ${MYSQL_BINARIES_DIR}`
	    touch $MYSQL_BINARIES_DIR"/${MYSQL_BINARIES_INSTALLED}"
	    if [ $? -ne 0 ] ; then
	       exit_error "Could write a file to $MYSQL_BINARIES_DIR" $ECHO_OUT
	    fi
	    echo "MySQL binaries installed in: $MYSQL_BINARIES_DIR" $ECHO_OUT
         else 
	    echo "MySQL binaries already installed in : $MYSQL_BINARIES_DIR" $ECHO_OUT
         fi 
else # move_binaries != 0
  echo "MySQL binaries already installed in: " $ECHO_OUT
    #create a file that is used by subsequent runs to find existing installations
         test -e $MYSQL_BINARIES_DIR"/${MYSQL_BINARIES_INSTALLED}" 
         if [ $? -ne 0 ] ; then
            `cp -r ${VERSION}/* ${MYSQL_BINARIES_DIR}`
	    touch $MYSQL_BINARIES_DIR"/${MYSQL_BINARIES_INSTALLED}"
	    if [ $? -ne 0 ] ; then
	       exit_error "Could write a file to $MYSQL_BINARIES_DIR" $ECHO_OUT
	    fi
         else
	    echo "MySQL binaries already installed in : $MYSQL_BINARIES_DIR" $ECHO_OUT
         fi 
fi  

  echo "$MYSQL_BINARIES_DIR" $ECHO_OUT
  clear_screen

}


#######################################################################
# CREATE MYSQL GROUP AND MYSQL USER ACCOUNT
#######################################################################

setup_mysql_datadir()
{
  if [ ! -d "$MYSQL_INSTALL_DIR" ] ; then
       mkdir $MYSQL_INSTALL_DIR
       if [ $? -ne 0 ] ; then
	  mkdir_error $MYSQL_INSTALL_DIR
       fi
  fi

  if [ $ROOTUSER -eq 1 ] ; then 
   MYSQL_USER="user           = $USERNAME"
   MYSQL_DATA_DIR=${MYSQL_INSTALL_DIR}/data
  else
  # Not ROOT
  # user-level mysql installation uses var as the data directory
  # and runs mysqld under the current user's id
  # Moving MySQL data directory from $MYSQL_DATA_DIR/data to $MYSQL_DATA_DIR/var.
  # This step is required by the mysql_install_db program.

    MYSQL_DATA_DIR=${MYSQL_INSTALL_DIR}/var

    if [ ! -d "${MYSQL_DATA_DIR}" ] ; then
        mkdir ${MYSQL_DATA_DIR}
        if [ $? -ne 0 ] ; then
  	  mkdir_error ${MYSQL_DATA_DIR}
        fi
    fi
  
    if [ -d "${MYSQL_INSTALL_DIR}/data" ] ; then
      rm -rf ${MYSQL_INSTALL_DIR}/data
      if [ $? -ne 0 ] ; then
        echo "" $ECHO_OUT
        echo "Couldn't delete the old data directory: ${MYSQL_INSTALL_DIR}/data"        $ECHO_OUT
        echo "When you install in user-mode, the data directory is changed from '\$MYSQL/data' to '\$MYSQL/var'"        $ECHO_OUT
      fi
    fi
    MYSQL_USER=
  fi

 touch $MYSQL_DATA_DIR"/$MYSQL_INSTALLED"
 if [ $? -ne 0 ] ; then
   echo ""
   echo "Does the directory exist: "
   echo "$MYSQL_DATA_DIR"
   echo "Does the user have permissions to write to this directory? "
   exit_error "Could not write to $MYSQL_DATA_DIR" $ECHO_OUT
 fi

}

setup_mysql_user_account()
{
  # set mysql datadir
 
  if [ $ROOTUSER -eq 1 ] ; then 
  
    MYSQL_USER_EXISTS=`cat /etc/passwd | grep $USERNAME`
    
    if [ "$MYSQL_USER_EXISTS" = "" ] ; then
        echo "-------------- Setting up User Account to Run Cluster --------------" $ECHO_OUT
        echo "" $ECHO_OUT
        echo -n "Create user account '$USERNAME' with home directory = '/home/$USERNAME'?" $ECHO_OUT
        if [ $NON_INTERACT -eq 0 ] ; then
           entry_ok
        else 
           eval false
        fi

        if [ $? -eq 0 ] ; then
          exit_error "You need to start cluster as a non-root user"
        fi

    	GROUP_EXISTS=`cat /etc/group | grep $USERNAME`
    	if [ "$GROUP_EXISTS" = "" ] ; then
  		groupadd $USERNAME
        fi
  	#if [ $? -eq 9 ] ; then
  	#    echo "" $ECHO_OUT
  	#    echo "Group already exists for $USERNAME" $ECHO_OUT
  	#    echo "" $ECHO_OUT
  	#elif [ $? -ne 0 ] ; then
  	#    echo "" $ECHO_OUT
  	#    echo "Failure: could not create '$USERNAME' group account." $ECHO_OUT
        #    echo "Workaround:"
        #    echo "Setup the user account and group before running this script." $ECHO_OUT
        #    echo "Then re-run the ndbinstaller script." $ECHO_OUT
	#    exit_error
  	#fi

# Here, we add the user account. We need to have a home directory for this
# user, as we need to store the ssh keys there. I am assuming you have bash
# available as a shell for this account.
# -g $USERNAME
       # useradd -s /bin/bash -g $USERNAME -d /home/$USERNAME -m $USERNAME
        useradd -s /bin/bash -g $USERNAME -m $USERNAME
        if [ $? -ne 0 ] ; then
   	   echo "" $ECHO_OUT
   	   echo "Failure: could not create $USERNAME user account" $ECHO_OUT
   	   echo "" $ECHO_OUT
   	   echo "Workaround: create the username for ndbinstaller with the 'useradd'" $ECHO_OUT
   	   echo "utility, and then re-run this ndbinstaller script." $ECHO_OUT
	   exit $COULD_NOT_CREATE_USER 
        fi

	echo "" $ECHO_OUT
	echo "Note: You will be prompted to create user accounts on all hosts in the cluster." 
	echo "      Use the same account name." $ECHO_OUT
	echo "      The password should be the same for all the $USERNAME accounts" $ECHO_OUT
	echo "      on all nodes in the cluster." $ECHO_OUT
	echo "" $ECHO_OUT
	passwd $USERNAME
    if [ $? -ne 0 ] ; then
           echo "" $ECHO_OUT
  	   echo "Error: could not change password of $USERNAME user account" $ECHO_OUT
  	   echo "After installation, change the password of $USERNAME user account using the 'passwd' program." $ECHO_OUT
        fi

        echo "" $ECHO_OUT
        echo "Created $USERNAME user account with homedir=/home/$USERNAME" $ECHO_OUT
	clear_screen  
   else 
        #user account exists
        echo "-------------- User Account Already Exists on this Host --------------" $ECHO_OUT
        echo "" $ECHO_OUT
        echo "A '$USERNAME' user account already exists." $ECHO_OUT
        echo "" $ECHO_OUT
	if [ $PARAM_USERNAME -eq 0 ] ; then
	   clear_screen
        else
	   clear
	fi
   fi

 fi


}



#######################################################################
# CREATE SYMBOLIC LINK
#######################################################################

setup_symbolic_link()
{
    NEW_LINK_CREATED=0

# $1 = source (full dir) $2 = target_dir/SYM_NAME $3 = SYM_NAME  
    create_symbolic_link() 
    {
	
    # get the name of the file/directory the symbolic link points to
	FILE_NAME=$2/$3
    # TODO: hide the output if FILE_NAME doesn't exist  2> /dev/null
	LS_OUT=$(ls -l "$FILE_NAME")
	TARGET=${LS_OUT#*-> }

    #if the symbolic link already exists for this directory, skip creating a symbolic link
	if [ "$1" = "$TARGET" ] ; then
	    echo "" $ECHO_OUT
	    echo "Symbolic link already exists from " $ECHO_OUT
            echo "$1 to:" $ECHO_OUT
            echo "$2/$3" $ECHO_OUT 
	    return 0
	fi
	NEW_LINK_CREATED=1
	if [ $# -ne 3 ] ; then
	    echo "Incorrect number of params to create_symbolic_link()" $ECHO_OUT
	fi
	echo "" $ECHO_OUT
	echo "It is recommended that you create a symbolic link" $ECHO_OUT
	echo "from $1" $ECHO_OUT
	echo "to   $2/$3" $ECHO_OUT
	echo "" $ECHO_OUT
        if [ $NON_INTERACT -eq 0 ] ; then
	    if [ -e $2/$3 ]  ; then
		entry_ok "Do you want to replace your existing symbolic link? (y/n/q) " 
	    else
		if [ -e $2/$3 ] ; then
		    exit_error "$2/$3 already exists and is not a symbolic link. Delete it manually using 'rm'."
		fi
		entry_ok "Do you want to create this symbolic link? (y/n/q) " 
	    fi
        else
            eval false
        fi

	if [ $? -eq 1 ] ; then  
	    rm -rf ${2}/${3} >& /dev/null
  	    ln -sf  ${1} ${2}/${3}
  	    if [ $? -ne 0 ] ; then 
		echo "" $ECHO_OUT
  		echo "Failed to create the symbolic link." $ECHO_OUT
  	    fi
	else 
	    echo "Not creating a symbolic link from:" $ECHO_OUT
  	    echo "$1 to" $ECHO_OUT
  	    echo "$2/$3" $ECHO_OUT
	fi
        
#	read ACCEPT
#	case $ACCEPT in
#            y | Y)
#		rm -rf ${2}/${3} >& /dev/null
#  		ln -sf  ${1} ${2}/${3}
#  		if [ $? -ne 0 ] ; then 
#		    echo "" $ECHO_OUT
#  		    echo "Failed to create the symbolic link." $ECHO_OUT
#  		fi
#		;;
#            n | N)
#		echo "Not creating a symbolic link from:" $ECHO_OUT
#  		echo "$1 to" $ECHO_OUT
#  		echo "$2/$3" $ECHO_OUT
#		;;
#            q | Q)
#		exit_error
#		;;
#            *)
#		echo "" $ECHO_OUT
#		echo "Please enter 'y', 'n', or 'q'." $ECHO_OUT
#		create_symbolic_link $1 $2 $3
#		;;
#	esac
    }

    echo "---- Setting up Symbolic links to MySQL Server and NDB Storage Directory ----" $ECHO_OUT
    create_symbolic_link $MYSQL_BINARIES_DIR $MYSQL_BASE_DIR "mysql"
    create_symbolic_link $NDB_DIR $NDB_INSTALL_DIR "ndb"
    if [ $NEW_LINK_CREATED -eq 0 ] ; then
	clear
    else
	clear_screen
    fi

}

#######################################################################
# SETUP CONNECTSTRING AND HOSTS
#######################################################################

# $1 hostname, $2 port
setup_connectstring()
{
#      `ifconfig | grep -e ".*inet addr:[0-9]*\.[0-9]*\.[0-9]\.[0-9]*" | sed -e 's/inet addr://g' | awk '{print $1}'` $ECHO_OUT

  if [ "$1" != "" ] && [ "$2" != "" ] ; then
      MGM_HOST=$1
      MGM_PORT=$2
      CONNECTSTRING=${MGM_HOST}:${MGM_PORT}
      return 0
  fi
  echo "-------------- Enter Management Server Details --------------" $ECHO_OUT

  if [ $MGM_HOST_NOT_SET -eq 1 ] ; then

      VALID_MGM=0
      #DISTRIBUTED=" (not 'localhost') "
      if [ $INSTALL_ACTION -eq $INSTALL_ANOTHER_MYSQLD_LOCALHOST ] ; then
	  DISTRIBUTED=0
      fi
      while [ $VALID_MGM -eq 0 ] ; do

	  if [ $INSTALL_ACTION -eq $INSTALL_MGM ] ; then
	      MGM_HOST=`hostname`
	      get_hostname() 
	      {
		  echo "" $ECHO_OUT
		  echo "Is '$MGM_HOST' the correct hostname for the Mgm server? (y/n) " $ECHO_OUT
		  read ACCEPT
		  case $ACCEPT in
		      y|Y|yes|YES|Yes)
	      ;;
	      n|N|no|NO|No)
                    echo "" $ECHO_OUT
		    enter_string "Enter the hostname/IP-addr for this Mgm Server:" 
		    MGM_HOST=$ENTERED_STRING
		    ;;
		    *)
		    echo "" $ECHO_OUT
		    echo "Enter 'y' or 'n'!" $ECHO_OUT
		    get_hostname
		    ;;
		    esac
	      }
	      get_hostname
	else

	  echo ""
	  enter_string "Enter the hostname/IP-addr for the Mgm Server:" 
          MGM_HOST=$ENTERED_STRING
	fi
	if [ $INSTALL_ACTION -ne $INSTALL_ANOTHER_MYSQLD_LOCALHOST ] ; then
	  if [ "$MGM_HOST" = $LOCALHOST ] ; then	  
	      echo "You cannot enter a hostname 'localhost' for the Management Server" $ECHO_OUT
	  elif [ "$MGM_HOST" = "" ] ; then
	      echo "You must enter a valid hostname for the Management Server" $ECHO_OUT
	  else
	      VALID_MGM=1
	  fi
	else
	  if [ "$MGM_HOST" = "" ] ; then
	      echo "You must enter a valid hostname for the Management Server" $ECHO_OUT
	  else
	      VALID_MGM=1
	  fi
	fi
      done

  fi      
  echo "Entered hostname is: $MGM_HOST" $ECHO_OUT

  if [ $MGM_PORT_NOT_SET -eq 1 ] && [ $NON_INTERACT -eq 0 ] ; then
      echo ""
      enter_string "Enter the port number for the ndb_mgmd : (default $DEFAULT_MGM_PORT)" 
      MGM_PORT=$ENTERED_STRING
      if [ "$MGM_PORT" = "" ] ; then
	  MGM_PORT=$DEFAULT_MGM_PORT
      fi
      echo "Entered portnumber is: $MGM_PORT" $ECHO_OUT
  fi

  CONNECTSTRING=${MGM_HOST}:${MGM_PORT}

  clear_screen
}

setup_mysqld_hostname()
{
 echo "--------------  MySQL Server Hostname Details --------------" $ECHO_OUT

 get_hostname() 
 {
	echo "" $ECHO_OUT
	echo "Is '$MYSQL_HOST' the correct hostname for this mysql server? (y/n) " $ECHO_OUT
	read ACCEPT
	    case $ACCEPT in
	       y|Y|yes|YES|Yes)
		 ;;
	       n|N|no|NO|No)
		    enter_string "Enter the hostname/IP-addr for this MySQL Server:" 
		    MYSQL_HOST=$ENTERED_STRING
		 ;;
	       *)
	       echo "" $ECHO_OUT
	       echo "Enter 'y' or 'n'!" $ECHO_OUT
	       get_hostname
	       ;;
	       esac
 }
if [ $MYSQL_HOST_NOT_SET -eq 1 ] ; then
 MYSQL_HOST=`hostname`
 get_hostname
 echo ""
 echo "MySQL Server Hostname = $MYSQL_HOST"
 clear_screen
fi

}

setup_ndbds()
{
       echo "-------------- Enter Management Server Details --------------" $ECHO_OUT
       echo "" $ECHO_OUT

       if [ "$PARAM_NUM_NODES" != "" ] ; then
	  NUM_NODES=$PARAM_NUM_NODES	  
       else
	  num_nodes() 
	  {
	      read ACCEPT
	      if [ "$ACCEPT" = "" ] ; then
		  ACCEPT=2
	      fi

	      case $ACCEPT in
		  1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16)
		      NUM_NODES=$ACCEPT
		      ;;
		  *)
		      echo "" $ECHO_OUT
		      echo "Enter a number of Data Nodes between 1 and 16." $ECHO_OUT
		      num_nodes
		      ;;
	      esac
	  }
	  echo "The number of Data Nodes in a cluster can be 2, 4, 8, 16, 1 (not recommended)" $ECHO_OUT
	  echo "Enter the number of Data Nodes (ndbd) in the cluster: (default 2)"
	  num_nodes
      fi

      echo "Number of Data Nodes in the cluster is: $NUM_NODES" $ECHO_OUT


      
      NUM_NODES_TO_START=$NUM_NODES
      count=`expr ${NUM_NODES}`
      num=0
      while [ $num -lt $count ] ; do
	  node_id=`expr $num + 1`

	  if [ ${NDB_HOST[$num]} != $LOCALHOST ] ; then
		echo "Data Node `expr $num + 1` will run on host:"
		echo "${NDB_HOST[$num]}"
		num=`expr $num + 1`
	  else
	      echo "" $ECHO_OUT
	      enter_string "Enter the hostname/IP-addr for Data Node $node_id :"
	      NDB_HOST[${num}]="$ENTERED_STRING"
	      if [ "$ENTERED_STRING" = "" ] ; then
		  echo "You must enter a valid hostname for the Data Node" $ECHO_OUT
		  NDB_HOST[${num}]=$LOCALHOST
	      elif [ "$ENTERED_STRING" = $LOCALHOST ] ; then	  
		  echo "You cannot enter a hostname 'localhost' for a Data Node" $ECHO_OUT
	      else
		  num=`expr $num + 1`
	      fi
	  fi
      done
    clear_screen
}

#######################################################################
# CREATE CLUSTER CONFIGURATION FILES
#######################################################################

setup_config_ini()
{ 
    echo "-------------- Starting Cluster Configuration --------------" $ECHO_OUT

   if [ $PARAM_CONFIG_DEFAULT -eq 1 ] ; then
       DATA_MEMORY="${DATA_MEMORY}M"
       INDEX_MEMORY=`expr $DATA_MEMORY / 5`"M"
       DISK_CHECKPOINT_SPEED_IN_RESTART=${DISK_CHECKPOINT_SPEED_IN_RESTART}M
       DISK_CHECKPOINT_SPEED=${DISK_CHECKPOINT_SPEED}M
       DISK_SYNC_SIZE=${DISK_SYNC_SIZE}M
       REDO_BUFFER=${REDO_BUFFER}M
       clear
       return 0
   fi

   setup_replicas()
   {

       echo -n "The number of Replicas is currently 2. Accept?" $ECHO_OUT
       if [ $NON_INTERACT -eq 0 ] ; then
           entry_ok $REPLICA_HELP
       else 
           eval false
       fi
       
       if [ $? -eq 0 ] ; then
	   enter_string "Enter the number of Replicas to be stored in each Data Node :" $REPLICA_HELP
	   NUM_REPLICAS=$ENTERED_STRING
       fi
   }
   if  [ $INSTALL_ACTION -eq $INSTALL_MGM ] || [ $INSTALL_ACTION -eq $INSTALL_LOCALHOST ] ; then
       setup_replicas
   fi
   
   echo "" $ECHO_OUT
   echo -n "Size of Data Memory for NDB nodes is 80MB. Accept?" $ECHO_OUT
   if [ $NON_INTERACT -eq 0 ] ; then
       entry_ok $DATA_MEMORY_HELP
   else 
       eval false
   fi

   if [ $? -eq 0 ] ; then  
       echo "" $ECHO_OUT
       printf 'Enter new Data Memory size (in MB): '
       read DM_SZ
       DATA_MEMORY=$DM_SZ
   fi
   
    # check if $DM_SZ is a number
  

   NUM_FRAGMENT_LOGFILES=`expr ${DATA_MEMORY} \* 6 / 64`
   REDO_LOG_SIZE=`expr ${NUM_FRAGMENT_LOGFILES} \* 64`

   
   if [ "$INDEX_MEMORY" == "" ] ; then 
       INDEX_MEMORY=`expr $DATA_MEMORY / 5`
   fi
    # Add the 'M' postfix, after using size to compute IM
   DATA_MEMORY="$DATA_MEMORY""M"
   
   echo "" $ECHO_OUT
   echo -n "Size of Index Memory for NDB nodes is $INDEX_MEMORY MB. Accept?"  $ECHO_OUT
   index_memory_help_setup

   if [ $NON_INTERACT -eq 0 ] ; then
       entry_ok $INDEX_MEMORY_HELP 
   else 
       eval false
   fi

   if [ $? -eq 0 ] ; then  
       echo "" $ECHO_OUT
       printf 'Enter new Index Memory size (in MB): '
       read IM_SZ
       INDEX_MEMORY=$IM_SZ
   fi
   INDEX_MEMORY=${INDEX_MEMORY}M


   num_fragment_logfiles_help_setup
   echo "" $ECHO_OUT
   echo -n "NumberOfFragmentLogFiles is $NUM_FRAGMENT_LOGFILES (64*$NUM_FRAGMENT_LOGFILES =  ${REDO_LOG_SIZE}MB). Accept?" $ECHO_OUT

   if [ $NON_INTERACT -eq 0 ] ; then
       entry_ok $NUM_FRAGMENT_LOGFILES_HELP
   else 
       eval false
   fi


   if [ $? -eq 0 ] ; then  
       echo "" $ECHO_OUT
       printf 'Enter new NoOfFragmentLogFiles (each file requires 64 MB disk space): '
       read NO_FRAGS
       NUM_FRAGMENT_LOGFILES=$NO_FRAGS
   fi

   REDO_LOG_SIZE=`expr ${NUM_FRAGMENT_LOGFILES} \* 64`
   if [ $? -ne 0 ] ; then
       echo "" $ECHO_OUT
       echo "NoOfFragmentLogFiles entered was not a valid number." $ECHO_OUT
       exit_error
   fi

   echo "" $ECHO_OUT
   echo -n "DiskCheckpointSpeed is 10MBytes/Sec.  Accept?" $ECHO_OUT

   if [ $NON_INTERACT -eq 0 ] ; then
       entry_ok $DISK_CHECKPOINT_SPEED_HELP
   else 
       eval false
   fi

   if [ $? -eq 0 ] ; then  
       echo "" $ECHO_OUT
       printf 'Enter new DiskCheckpointSpeed value (in MBytes/sec): '
       read DISK_CSPEED
       DISK_CHECKPOINT_SPEED=$DISK_CSPEED
   fi

   DISK_CHECKPOINT_SPEED_IN_RESTART=`expr ${DISK_CHECKPOINT_SPEED} \* 10`
   if [ $? -ne 0 ] ; then
       echo "" $ECHO_OUT
       echo "DiskCheckpointSpeed was not a valid number." $ECHO_OUT
       exit_error
   fi

   echo "" $ECHO_OUT
   echo -n "DiskCheckpointSpeedInRestart is ${DISK_CHECKPOINT_SPEED_IN_RESTART}MBytes/Sec. Accept?" $ECHO_OUT

   if [ $NON_INTERACT -eq 0 ] ; then
       entry_ok $DISK_CHECKPOINT_SPEED_IN_RESTART_HELP
   else 
       eval false
   fi

   if [ $? -eq 0 ] ; then  
       echo "" $ECHO_OUT
       printf 'Enter new DiskCheckpointSpeedInRestart value (in MBytes/sec): '
       read DISK_CSPEED
       DISK_CHECKPOINT_SPEED_IN_RESTART=$DISK_CSPEED
   fi


   echo "" $ECHO_OUT
   echo -n "DiskSyncSize is ${DISK_SYNC_SIZE}MBytes/Sec. Accept?" $ECHO_OUT

   if [ $NON_INTERACT -eq 0 ] ; then
       entry_ok $DISK_SYNC_SIZE_HELP
   else 
       eval false
   fi

   if [ $? -eq 0 ] ; then  
       echo "" $ECHO_OUT
       printf 'Enter new DiskSyncSize value (in MBytes/sec): '
       read DISK_CSPEED
       DISK_SYNC_SIZE=$DISK_CSPEED
   fi

   DISK_CHECKPOINT_SPEED_IN_RESTART=${DISK_CHECKPOINT_SPEED_IN_RESTART}M
   DISK_CHECKPOINT_SPEED=${DISK_CHECKPOINT_SPEED}M
   DISK_SYNC_SIZE=${DISK_SYNC_SIZE}M

   echo "" $ECHO_OUT
   echo -n "RedoBuffer size is ${REDO_BUFFER}MBytes/Sec. Accept?" $ECHO_OUT

   if [ $NON_INTERACT -eq 0 ] ; then
       entry_ok $REDO_BUFFER_HELP
   else 
       eval false
   fi

   if [ $? -eq 0 ] ; then  
       echo "" $ECHO_OUT
       printf 'Enter new RedoBuffer value (in MBytes/sec): '
       read buf
       REDO_BUFFER=$buf
   fi
   REDO_BUFFER=${REDO_BUFFER}M

   clear_screen
   echo "------- Summary of Configuration Parameters for MySQL Cluster -------" $ECHO_OUT

   echo "" $ECHO_OUT
   echo -e "NoOfReplicas\t\t\t\t $NUM_REPLICAS" $ECHO_OUT
   echo -e "DataMemory\t\t\t\t $DATA_MEMORY" $ECHO_OUT
   echo -e "IndexMemory\t\t\t\t $INDEX_MEMORY" $ECHO_OUT
   echo -e "NoFragmentLogFiles\t\t\t $NUM_FRAGMENT_LOGFILES (Redo Log size on disk: $REDO_LOG_SIZE MB)" $ECHO_OUT
   echo -e "DiskCheckpointSpeed\t\t\t $DISK_CHECKPOINT_SPEED" $ECHO_OUT
   echo -e "DiskCheckpointSpeedInRestart\t\t $DISK_CHECKPOINT_SPEED_IN_RESTART" $ECHO_OUT
   echo -e "DiskSyncSize\t\t\t\t ${DISK_SYNC_SIZE}" $ECHO_OUT  
   echo -e "RedoBuffer\t\t\t\t ${REDO_BUFFER}" $ECHO_OUT  

   echo "" $ECHO_OUT
   echo "To change these configuration parameters, edit the configuration file(s): " $ECHO_OUT  
   if [ $INSTALL_ACTION -eq $INSTALL_LOCALHOST ] ; then 
       echo "$NDB_DIR/config-4node.ini" $ECHO_OUT
       echo "$NDB_DIR/config-2node.ini" $ECHO_OUT
   else
       echo "$NDB_DIR/config-${NUM_NODES}node.ini" $ECHO_OUT
   fi
   echo "" $ECHO_OUT
   echo "Note: Perform a rolling restart for changes in these parameters to take effect." $ECHO_OUT  

   echo "" $ECHO_OUT
   echo -e "Note: \tChanges to 'NoOfReplicas', 'DataMemory', 'IndexMemory'" $ECHO_OUT
   echo -e "\tand 'NoFragmentLogFiles' require re-initialising the cluster with an" $ECHO_OUT
   echo -e "\t'$INIT_START' script in the \$NDB_HOME/scripts directory." $ECHO_OUT
   echo "" $ECHO_OUT
   
   clear_screen_no_skipline
}
 
setup_my_cnf()
{  
  if [ $PARAM_DEFAULT_MYSQL_SETTINGS -eq 0 ] ; then
    echo "-------------- MySQL Server Configuration Steps --------------" $ECHO_OUT
    echo "" $ECHO_OUT
    echo "Is there a mysqld running on this machine that was not installed by ndbinstaller.sh?"
    echo "If yes, change the socket to not clash with the socket for the existing mysqld!" $ECHO_OUT
    echo "The location for the MySQL socket is:" $ECHO_OUT
    echo "$MYSQL_SOCK"  $ECHO_OUT
    echo -n "Accept?"
    if [ $NON_INTERACT -eq 0 ] ; then
        entry_ok $MYSQL_SOCKET_HELP
    else 
        eval false
    fi

    if [ $? -eq 0 ] ; then
      enter_string "Enter the full pathname for the MySQL socket (e.g., /tmp/mysql.sock):" 
      MYSQL_SOCK=$ENTERED_STRING
      if [ ! -d `dirname $MYSQL_SOCK` ] ; then 
  	echo "" $ECHO_OUT
  	echo "Directory for socket filename does not exist." $ECHO_OUT
  	exit_error
      fi
    fi


    echo "" $ECHO_OUT   
    echo "Is there a mysqld running on this machine that was not installed by ndbinstaller.sh?"
    echo "If yes, change the port number to not clash with the existing mysqld port!" $ECHO_OUT
    echo "The port number for the mysqld is set to default (port $MYSQL_PORT)." $ECHO_OUT
    echo -n "Accept?"
    if [ $NON_INTERACT -eq 0 ] ; then
        entry_ok $MYSQL_PORT_HELP
    else 
        eval false
    fi
    if [ $? -eq 0 ] ; then
	  enter_string "Enter the port number for the mysqld :" 
	  MYSQL_PORT=$ENTERED_STRING
    fi

    echo "" $ECHO_OUT   
    echo "You need a binary log  to enable this MySQL server as a replication master." $ECHO_OUT   
    echo "Do not accept this option, unless you need it - logging consumes disk space." $ECHO_OUT   
    echo "Do you want to enable a binary log for the MySQL Server?" $ECHO_OUT
    echo -n "Accept?"
    if [ $NON_INTERACT -eq 0 ] ; then
        entry_ok $BINARY_LOG_HELP
    else 
        eval true
    fi

    if [ $? -eq 1 ] ; then
	binary_log_enable
    else
	binary_log_disable
    fi
  fi

  #MYSQL_PID="${MYSQL_INSTALL_DIR}/mysqld.pid"
  make_my_cnf
  if [ $PARAM_DEFAULT_MYSQL_SETTINGS -eq 0 ] ; then
      clear_screen
  else
      clear
  fi
      
}

# 
# Create the config.ini files
#
setup_config_scripts()
{  
    echo "--- Creating Cluster Config File (config.ini) ---" $ECHO_OUT
    echo "" $ECHO_OUT
    echo "Creating cluster configuration files:" $ECHO_OUT
    if [ $INSTALL_ACTION -eq $INSTALL_LOCALHOST ] ; then 
	echo "$NDB_DIR/config.ini" $ECHO_OUT
    else
	echo "$NDB_DIR/config-${NUM_NODES}node.ini" $ECHO_OUT
    fi

    if [ "$DISTRIBUTED" != "0" ] ; then
      make_config_ini $NUM_NODES
    else
      make_config_ini $NUM_NODES
      make_config_ini 2
    fi
  
    if [ ! -d $NDB_DIR/$MGM_DATADIR ] ; then
        mkdir $NDB_DIR/$MGM_DATADIR
        if [ $? -ne 0 ] ; then
  	mkdir_error $NDB_DIR/$MGM_DATADIR
        fi
    fi
  clear_screen
}

setup_ndb_datadirs()
{  
    if [ ! -d $NDB_DIR/$NDB_DATADIR ] ; then
         mkdir $NDB_DIR/$NDB_DATADIR
        if [ $? -ne 0 ] ; then
  	mkdir_error $NDB_DIR/$NDB_DATADIR
        fi
    fi

    if [ $INSTALL_ACTION -eq $INSTALL_NDB ] ; then
      
      echo "---------- Configuration for this Data Node ------------" $ECHO_OUT
      if [ "$NODEID" = "" ] ; then
         echo "" $ECHO_OUT
	 enter_string "Enter the Id for this Data Node (1, 2, ..):" $NODEID_HELP
	 NODEID=$ENTERED_STRING
      fi
      echo "Id for this Data Node is: $NODEID " $ECHO_OUT

      if [ ! -d $NDB_DIR/$NDB_DATADIR/$NODEID ] ;  then
         mkdir $NDB_DIR/$NDB_DATADIR/$NODEID
         if [ $? -ne 0 ] ; then
  	 mkdir_error $NDB_DIR/$NDB_DATADIR/$NODEID
         fi
      else
	echo "NDB Data directory already existed: " $ECHO_OUT
	echo "$NDB_DIR/$NDB_DATADIR/$NODEID" $ECHO_OUT
      fi
      clear_screen
    else 
      # It's a localhost or distributed cluster - create data directories for all nodes locally
      NODEID=0
      while [ $NODEID -lt $NUM_NODES ] ; do
        NODEID=$(($NODEID + 1))
        if [ ! -d $NDB_DIR/$NDB_DATADIR/$NODEID ] ;  then
             mkdir $NDB_DIR/$NDB_DATADIR/$NODEID
           if [ $? -ne 0 ] ; then
             mkdir_error $NDB_DIR/$NDB_DATADIR/$NODEID
           fi
        fi
      done
    fi
}

setup_chown_cluster()
{  
    if [ $ROOTUSER -eq 1 ] ; then
       	cd $NDB_DIR
         echo "-------------- Information Only --------------" $ECHO_OUT
         echo "" $ECHO_OUT
         echo "Changing directory ownership of $NDB_DIR to user '$USERNAME'" $ECHO_OUT
         chown -R $USERNAME *
	 if [ $? -ne 0 ] ; then
	     exit_error "Problem changing file ownership to the user account '$USERNAME'"
         fi
         chown -R $USERNAME .
	 chown -R $USERNAME $NDB_LOGS_DIR
         clear
    fi
}

#######################################################################
# FUNCTION TO GENERATE SSH Keys
#######################################################################

setup_username()
{
    if [ $ROOTUSER -eq 1 ] ; then    
	echo "--------- Choose the username that Cluster Processes will run as --------" $ECHO_OUT
	echo "" $ECHO_OUT
	echo "The default username that cluster will be run as is '$USERNAME'." $ECHO_OUT
	echo -n "Accept?"
	if [ $NON_INTERACT -eq 0 ] ; then
            entry_ok $USERNAME_HELP
	else 
            eval false
	fi
	if [ $? -eq 0 ] ; then
	    enter_string "Enter the username that will be used to run cluster:" $USERNAME_HELP
	    USERNAME=$ENTERED_STRING
	fi
	clear_screen
    fi
}

setup_sshdir()
{
   SSH_DIR=
   if [ $ROOTUSER -eq 1 ] ; then
      #SSH_DIR=/home/${USERNAME}/.ssh
      SSH_DIR=/${USERNAME}/.ssh
   else
      SSH_DIR=${HOMEDIR}/.ssh
   fi

   echo "" $ECHO_OUT
   if [ ! -d $SSH_DIR ] ; then
     echo "Creating a directory for ssh keys:" $ECHO_OUT
     echo "$SSH_DIR" $ECHO_OUT
     mkdir $SSH_DIR
     chown -R $USERNAME $SSH_DIR
     if [ $? -ne 0 ] ; then
	 mkdir_error $SSH_DIR
     fi
   else
       echo "Found a directory for ssh keys:" $ECHO_OUT
       echo "$SSH_DIR" $ECHO_OUT
   fi

}


setup_ssh_on_mgmd()
{ 
   echo "--- Setup SSH to be able to start all nodes in the cluster from this Host ---" $ECHO_OUT
   echo "" $ECHO_OUT

   setup_sshdir

   if [ $FORCE_GENERATE_SSH_KEY -eq 0 ] ; then
       if [ -e $SSH_DIR/id_rsa ] ; then
	   echo "A public key already exists for user '$USERNAME'. Not generating a new key."
	   START_WITH_SSH=1
	   clear
	   return 0
       fi
   fi

   echo "Generating a public key for user $USERNAME using ssh-keygen." $ECHO_OUT
   echo "" $ECHO_OUT
   echo "A public key is used to allow you to start the Data Nodes on remote hosts" $ECHO_OUT
   echo "using the scripts installed on this management server host." $ECHO_OUT
   echo ""
   echo -e "Requirements to generate a key: ssh-keygen, scp, ssh.\n" $ECHO_OUT

   echo "" $ECHO_OUT
   echo "Your passphrase must be at least 8 characters long (and not easily guessable)." $ECHO_OUT
   echo "" $ECHO_OUT
   if [ $ROOTUSER -eq 1 ] ; then
       ssh-keygen -t rsa -f ${SSH_DIR}/id_rsa
   else
       ssh-keygen -t rsa -f ${SSH_DIR}/id_rsa
   fi
       
   if [ $? -ne 0 ] ; then
       exit_error "There was a problem when running the program 'ssh-keygen'."
   fi
   echo "" $ECHO_OUT
   echo "A private key file was stored in $SSH_DIR/id_rsa" $ECHO_OUT
   echo "A public key file was stored in $SSH_DIR/id_rsa.pub" $ECHO_OUT
   echo "" $ECHO_OUT
   echo "You can change your passphrase using: ssh-keygen -p $SSH_DIR/id_rsa" $ECHO_OUT
   echo "" $ECHO_OUT
   echo "Appending id_rsa.pub to $SSH_DIR/authorized_keys" $ECHO_OUT
   cat ${SSH_DIR}/id_rsa.pub >> ${SSH_DIR}/authorized_keys
   if [ $? -ne 0 ] ; then
       exit_error "There was a problem writing id_rsa.pub to ${SSH_DIR}/authorized_keys."
   fi

  # for commercial-ssh do the following
  # echo "key id_rsa.pub" >> authorization
   chmod go-w . ${SSH_DIR}/authorized_keys
   if [ $? -ne 0 ] ; then
       exit_error "There was a problem when changing privileges of authorized_keys file."
   fi
   if [ $ROOTUSER -eq 1 ] ; then
       chown -R $USERNAME $SSH_DIR
       if [ $? -ne 0 ] ; then
	   exit_error "There was a problem when changing ownership of $SSH_DIR."
       fi
   fi
  
  #Each time you will login using ssh, you will be asked for your passphrase. If you want to
  #enable a login session without entering the passphrase each time you should write:
  #eval `ssh-agent`
  #ssh-add
  #<4|1>daniel51@mangal:~>eval `ssh-agent`
  #Agent pid 11693
  #<4|1>daniel51@mangal:~> ssh-add
  #Enter passphrase for /cs/grad/daniel51/.ssh/id_rsa:
  #Identity added: /cs/grad/daniel51/.ssh/id_rsa
  #(/cs/grad/daniel51/.ssh/id_rsa)
  #!/usr/bin/expect -f
  #
  #set UserID "$USERNAME"
  #set Passphrase "cluster"
  #set remotehost "jimscomputer"
  #spawn ssh -l $UserID $remotehost
  #expect -re "Enter passphrase for key '.*':"
  #send "$Passphrase\r"
  #interact

  START_WITH_SSH=1
  clear_screen
  
}


setup_ssh_on_ndbd()
{ 
    echo "--------- Setup SSH to be able to start this node from the Mgm Host ---------" $ECHO_OUT
    echo "" $ECHO_OUT
    echo "Generate a ssh-key." $ECHO_OUT
    echo "An ssh-key allows you to start the cluster from a single host" $ECHO_OUT
    echo "using init and start scripts installed on the management server host." $ECHO_OUT
    echo -e "Requirements: ssh-keygen, scp, ssh.\n" $ECHO_OUT
    echo -n "Acquire the public ssh-key from the Management Server node?" $ECHO_OUT
    if [ $NON_INTERACT -eq 0 ] ; then
	entry_ok $SSH_HELP
    else 
	eval false
    fi

    if [ $? -eq 0 ] ; then
	clear_screen
	return
    fi

    PUB_KEY=id_rsa.pub
    PUB_KEY_TMP=${PUB_KEY}_tmp


    setup_sshdir

# If localhost == $MGM_HOST, skip all this
    echo "" $ECHO_OUT
    echo -n "Is the Management Server located on the current host?" $ECHO_OUT
    if [ $NON_INTERACT -eq 0 ] ; then
	entry_ok 
    else 
	eval true
    fi

    if [ $? -eq 0 ] && [ $SKIP_DOWNLOAD_MGM_SSH_KEY -eq 0 ] ; then
	
	echo "" $ECHO_OUT
	echo "If you have generated the SSH key when you installed the Management Server," $ECHO_OUT
	echo "the public RSA key should be available on the Management Server, $MGM_HOST, at:" $ECHO_OUT
	echo "/home/$USERNAME/.ssh/${PUB_KEY}" $ECHO_OUT
	echo -n "Is this the correct path for the ${PUB_KEY} file?"
	if [ $NON_INTERACT -eq 0 ] ; then
	    entry_ok $AUTHORIZED_KEYS_HELP
	else 
	    eval false
	fi
	
	if [ $? -eq 0 ] ; then
	    echo "Enter the ssh directory: " $ECHO_OUT
	    read dir
	    SSH_DIR=$dir
	else
      # ~${USERNAME} did not work for some reason....
	   if [ $ROOTUSER -eq 1 ] ; then
	     SSH_DIR=/${USERNAME}/.ssh
           else 
	     SSH_DIR=/home/${USERNAME}/.ssh
           fi
	fi
	echo "" $ECHO_OUT

	test -e ${SSH_DIR}/authorized_keys

	if [ $? -ne 0 ] ; then
	    touch ${SSH_DIR}/authorized_keys
	fi
	echo "Local authorized_keys found here: ${SSH_DIR}/authorized_keys" $ECHO_OUT
	echo "Using scp to copy ${PUB_KEY} from Mgmt Server." $ECHO_OUT
	`scp ${USERNAME}@${MGM_HOST}:${SSH_DIR}/${PUB_KEY} ${SSH_DIR}/${PUB_KEY_TMP}`
	if [ $? -ne 0 ] ; then
	    echo ""
	    echo "Error number: $? for scp"
	    echo "Error: you probably do not have an ssh key for ${MGM_HOST}."
	    echo "Log in using ssh to get the ssh key:"
	    echo "ssh ${USERNAME}@${MGM_HOST}"
  	    exit_error ""
	fi
	cat ${SSH_DIR}/${PUB_KEY_TMP} >> ${SSH_DIR}/authorized_keys && rm ${SSH_DIR}/${PUB_KEY_TMP}
	if [ $ROOTUSER -eq 1 ] ; then
	    chown $USERNAME ${SSH_DIR}/*
	fi
  # check for error during 'scp'
	if [ $? -ne 0 ] ; then
	    echo "" $ECHO_OUT
	    echo "Error: Could not copy ssh-key from MGM server using 'scp' program" $ECHO_OUT
	    echo "" $ECHO_OUT
	    echo "Potential Causes of Error;" $ECHO_OUT
	    echo "0. You probably don't have a ssh-key for the mgm server. Run (then, re-run script):"
	    echo "   su mysql"
	    echo "   ssh ${USERNAME}@${MGM_HOST}"
	    echo ""
	    echo "1. Check first that the Management Server host is running: $MGM_HOST" $ECHO_OUT
	    echo "" $ECHO_OUT
	    echo "2. Have you installed the Management Server on $MGM_HOST ? You must do this first." $ECHO_OUT
	    echo "" $ECHO_OUT
	    echo "3. Is the 'scp' program installed on this host?" $ECHO_OUT
	    echo "" $ECHO_OUT
	    echo "4. You may have stale ssh keys for the host: $MGM_HOST" $ECHO_OUT
	    echo "   If so, remove the files: ${SSH_DIR}/authorized_keys and ${SSH_DIR}/known_hosts" $ECHO_OUT
	    echo "" $ECHO_OUT
	    echo "Workaround:" $ECHO_OUT
	    echo "After this script has finished, you copy the file: " $ECHO_OUT
	    echo "~$USERNAME/.ssh/authorized_keys from the remote host '${MGM_HOST}'" $ECHO_OUT
	    echo "to your local directory: ~$USERNAME/.ssh/" $ECHO_OUT
	    echo "Then, re-run this installer, and do not select the option to copy ssh-keys" $ECHO_OUT
	    echo "from the management server during install." $ECHO_OUT
	    exit_error 
	fi
	
	if [ $ROOTUSER -eq 1 ] ; then
	    chmod go-w . ${SSH_DIR}/authorized_keys
	else
	    chmod go-w . ${SSH_DIR}/authorized_keys
	fi
	if [ $? -ne 0 ] ; then
	    exit_error "Could not change permissions for ${SSH_DIR}/authorized_keys" $ECHO_OUT
	fi
	START_WITH_SSH=1  
    fi
    if [ $SKIP_DOWNLOAD_MGM_SSH_KEY -eq 0 ] ; then
	START_WITH_SSH=1  
    fi
    clear_screen  
}


#######################################################################
# MYSQLD INSTALL: INITIAL DATABASE AND CHANGE DIRECTORY OWNERSHIP 
#######################################################################

setup_db_ownership()
{
  
   echo "------ Initialising the MySQL Server by Installing Default Databases  ------" $ECHO_OUT
   echo "" $ECHO_OUT  
   if [ $ROOTUSER -eq 1 ] ; then
        echo "" $ECHO_OUT
        echo "Changing ownership of files in:" $ECHO_OUT
        echo "$MYSQL_BINARIES_DIR" $ECHO_OUT
        echo "to 'root'." $ECHO_OUT
        echo "" $ECHO_OUT
	echo "Changing ownership of the data directory $MYSQL_DATA_DIR to user '$USERNAME'. " $ECHO_OUT

	cd $MYSQL_BINARIES_DIR
        chown -R root *
	if [ $? -ne 0 ] ; then
	     exit_error "Problem changing file ownership of $MYSQL_BINARIES_DIR to the 'root' user"
        fi

        chown -R root .
	if [ $? -ne 0 ] ; then
	     exit_error "Problem changing file ownership of directory $MYSQL_BINARIES_DIR to the 'root' user"
        fi

        chgrp -R $USERNAME .
	if [ $? -ne 0 ] ; then
	     exit_error "Problem changing file ownership to the user account '$USERNAME'"
        fi

	chown -R $USERNAME $MYSQL_DATA_DIR
	if [ $? -ne 0 ] ; then
	     exit_error "Problem changing file ownership to the user account '$USERNAME'"
        fi

	chown -R $USERNAME $MYSQL_INSTALL_DIR
	if [ $? -ne 0 ] ; then
	     exit_error "Problem changing file ownership to the user account '$USERNAME'"
        fi

   fi

   # msyql_install_db can be in either bin/ or scripts (default)
   MYSQL_INSTALL_DB="scripts/mysql_install_db"

   if [ ! -e $MYSQL_BINARIES_DIR/$MYSQL_INSTALL_DB ] ; then

       if [ ! -e $MYSQL_BINARIES_DIR/bin/mysql_install_db ] ; then
	   exit_error "Could not find mysql_install_db script in either /scripts or /bin dirs."
       else
	   MYSQL_INSTALL_DB="bin/mysql_install_db"
       fi
   fi
   echo "" $ECHO_OUT
   echo "Creating default mysql databases using command:" $ECHO_OUT
   echo "\$MYSQL_BIN/mysql_install_db --defaults-file=$MY_CNF" $ECHO_OUT
   #echo "\$MYSQL_BIN/mysql_install_db --defaults-file=$MY_CNF --force" $ECHO_OUT
   cd $MYSQL_BINARIES_DIR  
  # TODO: replace scripts/... with bin/..
   $MYSQL_INSTALL_DB --defaults-file=$MY_CNF --force 
# --force causes mysql_install_db to run even if DNS does not work. In that case, grant table entries that normally use host names will use IP addresses.
# >& /dev/null
  
   if [ $? -ne 0 ] ; then
     echo "" $ECHO_OUT
     echo "Failure: problem installing default mysql database." $ECHO_OUT
     echo "" $ECHO_OUT
     exit_error
   fi 

   echo "" $ECHO_OUT
   echo "Finished installing default DB and settting directory ownership privileges" $ECHO_OUT
   clear_screen
}
 
  
#######################################################################
# COPY START-/STOP-NDB SCRIPTS TO $MYSQL_BINARIES_DIR/scripts
#######################################################################

chown_start_scripts()
{
    cd ${NDB_DIR}/scripts/

    if [ $ROOTUSER -eq 1 ] ; then
        chown -R $USERNAME *
	chown -R $USERNAME .
    fi

    chmod +x *.sh
}

localhost_start_scripts()
{
    make_localhost_scripts

    make_shutdown_cluster

    make_rolling_restart 2
    make_rolling_restart 4

    make_single_user_mode

    make_memory_usage

    chown_start_scripts  

    echo $README > $MYSQL_INSTALL_DIR/README.txt
 }

mgm_start_scripts()
{

    make_mgm_scripts

    make_shutdown_cluster

    make_start_mgmd $NUM_NODES

    make_rolling_restart $NUM_NODES

    make_single_user_mode

    make_start_backup

#    make_ndb_restore

    make_memory_usage

    chown_start_scripts  
 }


ndbd_start_scripts()
{
    make_start_ndbd

    chown_start_scripts

 }

 
# $1 = "add_startup"
update_startup_scripts_mysqld()
{  
    if [ "$START_SCRIPTS" = "" ] && [ "$SHUTDOWN_SCRIPTS" = "" ] ; then
	return 0
    fi

    MYSQL_NUMBER=`expr $NUM_MYSQLS + 1`
    
    # Make script to extract Mysql port number from the my.cnf config file
    make_get_mysql_port "$MYSQL_NUMBER"

    make_mysqld "$MYSQL_NUMBER"

    make_start_mysql_client "$MYSQL_NUMBER"

    UPDATE_SCRIPTS=0
    update_scripts () 
    {
	echo "" $ECHO_OUT
	if [ $UPDATE_SCRIPTS -eq 1 ] && [ "$INSTALL_ACTION" == "$INSTALL_MYSQLD" -o "$INSTALL_ACTION" == "$INSTALL_ANOTHER_MYSQLD" ] ; then
	    echo "Note: This step will require entering your password 4 times." $ECHO_OUT
	fi
	printf 'Add start/stop to the MySQL Server startup/shutdown scripts? (y/n) ' $ECHO_OUT
	read ACCEPT
	case $ACCEPT in
            y | Y)
		UPDATE_SCRIPTS=1
		;;
            n | N)
		;;
            q | Q)
		exit_error
		;;
            *)
		echo "" $ECHO_OUT
		echo "Please enter 'y', 'n', or 'q'." $ECHO_OUT
		update_scripts
		;;
	esac
    }
    if [ "$1" = "add_startup" ]  ; then
        UPDATE_SCRIPTS=1
    else
        if [ $NON_INTERACT -eq 0 ] ; then
	    echo "------- Patch MySQL Cluster Init/Start/Shutdown Scripts? --------" $ECHO_OUT
	    update_scripts
        else 
            UPDATE_SCRIPTS=1
        fi
    fi

      # -o is the same as [ ] || [ ] 
    if [ $UPDATE_SCRIPTS -eq 1 ] && [ "$INSTALL_ACTION" == "$INSTALL_LOCALHOST_MYSQLD" -o "$INSTALL_ACTION" == "$INSTALL_ANOTHER_MYSQLD_LOCALHOST" ] 
        then
	echo "
# Start MySQL Server Number `expr $NUM_MYSQLS + 1`

if [ \"\$1\" = \"0\" ] ; then
echo \"Starting MySQL Server Number `expr $NUM_MYSQLS + 1`\"
  $MYSQL_BINARIES_DIR/bin/mysqld --defaults-file=$MY_CNF > $NDB_LOGS_DIR/mysql-stdout-`expr $NUM_MYSQLS + 1`.log 2>&1 > /dev/null &
else
  echo \"Skipping starting MySQL Server `expr $NUM_MYSQLS + 1`\"
fi

" >> $MYSQLD_START_SCRIPT

        for stop in $SHUTDOWN_SCRIPTS ; do	
	    echo "
if [ \$SKIP_MYSQLDS -eq 0 ] ; then
	    echo \"Stopping MySQL Server Number `expr $NUM_MYSQLS + 1`\"
	    PORT=\`$CHANGE_PORT_SCRIPT\`
	    $MYSQL_BINARIES_DIR/bin/mysqladmin -h $MYSQL_HOST --protocol=tcp --port=\$PORT -u root shutdown
else
  echo \"Skipping stopping MySQL Server Number `expr $NUM_MYSQLS + 1`\"
fi
                 " >> $stop
        done

      # it's a remote MYSQLD
    elif [ $UPDATE_SCRIPTS -eq 1 ] && [ "$INSTALL_ACTION" == "$INSTALL_MYSQLD" ] || [ "$INSTALL_ACTION" == "$INSTALL_ANOTHER_MYSQLD" ] 
    then

	setup_sshdir
        echo ""
        echo "scp'ing MySQL start/stop scripts from $MGM_HOST to /tmp/ dir for update."
        echo ""
	  # copy the startup/shutdown files from mgm server to a local directory. edit them, copy them back.
	R_MYSQL_SHUTDOWN_TMP="${NDB_DIR}/scripts/shutdown-cluster.sh"
	R_MYSQL="$MYSQLD_START_SCRIPT"


	echo "Downloading mysqld-startup script from $MGM_HOST..."
	echo ""
        `scp $USERNAME@${MGM_HOST}:${R_MYSQL} ${MYSQLSTART_TMP}`
	if [ $? -ne 0 ] ; then
	    exit_error "Problem scp'ing to $MGM_HOST and copying file ${R_MYSQL}"
	fi

	echo "Downloading mysqld-shutdown script from $MGM_HOST..."
	echo ""
        `scp $USERNAME@${MGM_HOST}:${R_MYSQL_SHUTDOWN_TMP} ${MYSQL_SHUTDOWN_TMP}`
	if [ $? -ne 0 ] ; then
	    exit_error "Problem scp'ing to $MGM_HOST and copying file ${R_MYSQL_SHUTDOWN_TMP}"
	fi

 # edit the startup scripts

        echo "Patching scripts to include this mysqld in startup/shutdown...."
        echo ""	  


	echo "" >> $MYSQLSTART_TMP
	echo "# Starting MySQL Server at host $MYSQL_HOST" >> $MYSQLSTART_TMP
	echo "" >> $MYSQLSTART_TMP
	echo "
if [ \"\$1\" = \"0\" ] ; then
  echo \"Starting MySQL Server at host $MYSQL_HOST\"
  ssh ${USERNAME}@${MYSQL_HOST} ${MYSQLD_STARTER}
else
  echo \"Skipping stopping MySQL Server at host $MYSQL_HOST\"
fi
" >> $MYSQLSTART_TMP


# edit the shutdown scripts update
	echo "
if [ \$SKIP_MYSQLDS -eq 0 ] ; then
	    echo \"Stopping MySQL Server Number at $HOSTNAME\"
	    ssh ${USERNAME}@${MYSQL_HOST} ${MYSQLD_STOPPER}
else
  echo \"Skipping stopping MySQL Server Number at $HOSTNAME\"
fi
                 " >> ${MYSQL_SHUTDOWN_TMP}


	  # copy them back to mgm server
        echo "Now going to copy the updated startup/shutdown scripts back to $MGM_HOST"

	echo ""
	echo "Uploading start-mysqlds script..."
	echo ""
        `scp ${MYSQLSTART_TMP} $USERNAME@${MGM_HOST}:${R_MYSQL}`
	if [ $? -ne 0 ] ; then
	    exit_error "Problem scp'ing to $MGM_HOST and copying to file ${R_MYSQL}"
	fi

	echo ""
	echo "Uploading stop-mysqlds script..."
	echo ""
        `scp ${MYSQL_SHUTDOWN_TMP} $USERNAME@${MGM_HOST}:${R_MYSQL_SHUTDOWN_TMP}`
	if [ $? -ne 0 ] ; then
	    exit_error "Problem scp'ing to $MGM_HOST and copying file ${R_MYSQL_SHUTDOWN_TMP}"
	fi
	
    else
	exit_error "Something went wrong patching mysqld scripts"
    fi


    if [ "$INSTALL_ACTION" != "$INSTALL_LOCALHOST_MYSQLD" ] ; then
	clear_screen
    else
	clear
    fi
 }

MGM_RUNNING=0 
is_mgmd_running() 
{
      echo "" $ECHO_OUT
      echo "Testing to see if a cluster is already running on host '$CONNECTSTRING'" $ECHO_OUT
      echo "..." $ECHO_OUT
    ${MYSQL_BINARIES_DIR}/bin/ndb_mgm -c $CONNECTSTRING -t $1 -e show >& /dev/null

    if [ $? -eq 0 ] ; then
	MGM_RUNNING=1
	echo "" $ECHO_OUT     
        echo "A management server is already running on $CONNECTSTRING" $ECHO_OUT
	echo "" $ECHO_OUT	
	echo "You should shutdown your existing cluster using \$NDB_HOME/scripts/shutdown.sh" $ECHO_OUT
    else
	echo "There is no management server currently running on $CONNECTSTRING" $ECHO_OUT
	MGM_RUNNING=0
	return 0
    fi
    return 1
}

start_mysqlds()
{  

     START_MYSQLD=0
     start_mysqld() 
     {
      if [ $ROOTUSER -eq 1 ] ; then
	  exit 0
      fi

      echo "-------------- Start the MySQL Server Running --------------" $ECHO_OUT

      is_mgmd_running "2"

      echo "" $ECHO_OUT
      if [ $MGM_RUNNING -eq 0 ] ; then
	  printf 'There is no cluster running. Start your MySQL Server (mysqld) now? (y/n) '
      else
	  echo "A mgmd appears to be running." $ECHO_OUT
	  printf 'Do you want to start your mysqld now and join the cluster? (y/n) '
      fi

      read ACCEPT
       case $ACCEPT in
        y | Y)
          START_MYSQLD=1
          ;;
        n | N)
          START_MYSQLD=0
          ;;
        *)
          echo "" $ECHO_OUT
          echo "Please enter 'y' or 'n'." $ECHO_OUT
          start_mysqld
          ;;
       esac
     }
    start_mysqld
  
    if [ $START_MYSQLD -eq 1 ] ; then
       echo "" $ECHO_OUT
       echo "Executing: \$MYSQL_BIN/bin/mysqld --defaults-file=$MY_CNF > $NDB_LOGS_DIR/mysql-stdout-`expr $NUM_MYSQLS + 1`.log 2>&1 > /dev/null &" $ECHO_OUT
       ${MYSQL_BINARIES_DIR}/bin/mysqld --defaults-file=$MY_CNF > $NDB_LOGS_DIR/mysql-stdout-`expr $NUM_MYSQLS + 1`.log 2>&1 > /dev/null &
    fi

}
  
start_ndbds()
{
    echo "-------------- Start the Data Node Running  --------------" $ECHO_OUT

     START_NDB=0
     start_ndbd() 
     {
      echo "" $ECHO_OUT
      echo "Is the management server (ndb_mgmd) running?" $ECHO_OUT
      echo "The ndb_mgmd must be running to allow a data node join the cluster." $ECHO_OUT
      printf 'Do you want to start your Data Node (ndbd) now? (y/n) '
      read ACCEPT
       case $ACCEPT in
        y | Y)
          START_NDB=1
          ;;
        n | N)
          START_NDB=0
          ;;
        *)
          echo "" $ECHO_OUT
          echo "Please enter 'y' or 'n'." $ECHO_OUT
          start_ndbd
          ;;
       esac
     }
    start_ndbd
  
    if [ $START_NDB -eq 1 ] ; then
#      is_mgmd_running "2"
      
      if [ $ROOTUSER -eq 1 ] ; then
          echo "" $ECHO_OUT
         echo "You should login as user '$USERNAME' and start the ndbd nodes." $ECHO_OUT
      fi
      if [ $MGM_RUNNING -eq 1 ] ; then
        echo "" $ECHO_OUT
        echo "Executing: \$NDB_DIR/scripts/$NDBD_INIT-${NODEID}.sh" $ECHO_OUT
       ${NDB_DIR}/scripts/$NDBD_INIT-${NODEID}.sh
      else
        echo "" $ECHO_OUT
        echo "The management server does not appear to be running on: $CONNECTSTRING" $ECHO_OUT        
        echo "No attempt made to start 'ndbd'." $ECHO_OUT
      fi
    fi
    clear_screen
 }
  
start_cluster()
{
    echo "-------------- Start MySQL Cluster Running --------------" $ECHO_OUT
    START_CLUSTER=0
     start_it() 
     {
      echo "" $ECHO_OUT
      printf 'Do you want to try to start running your cluster now? (y/n) '
      read ACCEPT
       case $ACCEPT in
        y | Y)
          START_CLUSTER=1
          ;;
        n | N)
          START_CLUSTER=0
          ;;
        *)
          echo "" $ECHO_OUT
          echo "Please enter 'y' or 'n'." $ECHO_OUT
          start_it
          ;;
       esac
     }
     start_it
  
    if [ $START_CLUSTER -eq 1 ] ; then

      if [ $ROOTUSER -eq 1 ] ; then
        echo "" $ECHO_OUT
        echo "You should login as user '$USERNAME' and start the cluster" $ECHO_OUT
      else

       start_num_nodes() 
       {
           echo "" $ECHO_OUT
            printf 'Enter the cluster size (number of Data Nodes) you want to start: (2/4) '
           read ACCEPT
           case $ACCEPT in
              2|4)
                NUM_NODES_TO_START=$ACCEPT
                ;;
              *)
              echo "" $ECHO_OUT
              echo "The valid number of Data Nodes  is '2' or '4'." $ECHO_OUT
              start_num_nodes
              ;;
              esac
        }
       if [ $INSTALL_ACTION -eq $INSTALL_LOCALHOST ] && [ $MGM_RUNNING -eq 0 ]  ; then
           #start_num_nodes
           NUM_NODES_TO_START=4
           echo "" $ECHO_OUT
           echo "Starting the cluster with $NUM_NODES_TO_START data nodes by running:" $ECHO_OUT 
           echo "\$NDB_HOME/scripts/$INIT_START-${NUM_NODES_TO_START}${CLUSTER_START}" $ECHO_OUT
           echo "" $ECHO_OUT
           ${NDB_DIR}/scripts/$INIT_START-${NUM_NODES_TO_START}${CLUSTER_START}  
           #>> $INSTALL_LOG 
           # wait for the mysqld to join as well
           if [ $? -eq 2 ] ; then
               exit_error "Could not start localhost cluster."
           fi
           sleep 1
       fi
     fi
   fi
    

#    if [ $START_CLUSTER -eq 1 ] && [ $ROOTUSER -eq 0 ] ; then
#     clear_screen
#     echo "=================================================================" $ECHO_OUT    
#     echo "Here we run the following script to examine the state of the cluster: " $ECHO_OUT
#     echo "\$NDB_HOME/scripts/$MGM_CLIENT_START -e show" $ECHO_OUT
#     echo "" $ECHO_OUT
     # print out the state of the cluster
#     ${MYSQL_BINARIES_DIR}/bin/ndb_mgm -c $CONNECTSTRING -e "show" -t $MGM_STARTUP_TIME
#    fi
   clear_screen_no_skipline
}



###############################################################################################
# INSTALL SERVICES IN /etc/rc.d
###############################################################################################
install_services()
{
    if [ $ROOTUSER -eq 0 ] ; then    
       return 0
    fi

    PROCESS=
    PID=
    CONNECTION_TEST=
    case $INSTALL_ACTION in
       $INSTALL_MGM)
        PROCESS="ndb_mgmd"
        PID="63"
        CONNECTION_TEST="  if failed host 127.0.0.1 port $MGM_PORT then restart
   if 5 restarts within 5 cycles then timeout"
         ;;
        $INSTALL_NDB)
        PROCESS="ndb_node-$NODEID"
        PID="$NODEID"
         ;;
        $INSTALL_MYSQLD | $INSTALL_ANOTHER_MYSQLD | $INSTALL_LOCALHOST_MYSQLD)
        PROCESS="mysql.server"
         ;;
        *)
         echo "Invalid install action active, when install_service called"
         exit 2
    esac


    echo "-------------- Install Processes as Startup Services --------------" $ECHO_OUT
    INSTALL_SERVICES=0
     start_process_as_service() 
     {
      echo "" $ECHO_OUT
      echo "Install a startup-script for $PROCESS in /etc/init.d ? (y/n)"
      read ACCEPT
       case $ACCEPT in
        y | Y)
          INSTALL_SERVICES=1
          ;;
        n | N)
          INSTALL_SERVICES=0
          ;;
        *)
          echo "" $ECHO_OUT
          echo "Please enter 'y' or 'n'." $ECHO_OUT
          start_process_as_service
          ;;
       esac
     }
    if [ $NON_INTERACT -eq 0 ] ; then
     start_process_as_service
    fi
 
    if [ $INSTALL_SERVICES -eq 0 ] ; then
       clear_screen
       return 1
    fi


    case $INSTALL_ACTION in
       $INSTALL_MGM)
         make_initd_mgmd
         ;;
        $INSTALL_NDB)
         make_initd_ndbd
         ;;
        $INSTALL_MYSQLD | $INSTALL_ANOTHER_MYSQLD | $INSTALL_LOCALHOST_MYSQLD)
         make_initd_mysqld
         ;;
        *)
         echo "Invalid install action active, when install_service called"
         exit 2
    esac


    USE_MONIT=0
    use_monit() 
    {
           echo "" $ECHO_OUT
           echo "You can use a 3-rd party service called 'Monit' to manage process failures." $ECHO_OUT
           echo "Use monitrc to monitor/restart the $PROCESS process ? (y/n/h)"
           read ACCEPT
           case $ACCEPT in
               y | Y)
                   USE_MONIT=1
                   ;;
               n | N)
                   USE_MONIT=0
                   ;;
               h | H)
                   echo ""
                   echo "Monit code will be generated that can be added to /etc/monit/monitrc"
                   echo ""
                   use_monit
                   ;;
               *)
                   echo "" $ECHO_OUT
                   echo "Please enter 'y' or 'n'." $ECHO_OUT
                   use_monit
                   ;;
           esac
    }
    if [ $NON_INTERACT -eq 0 ] ; then
       use_monit
       if [ $USE_MONIT -eq 0 ] ; then
          clear_screen
          return 0
       fi
    else 
        return 0
    fi

    MONIT_INSTALLED=0
    # if we find the monitrc file, or we're using debian/ubuntu
    if [ -e "/etc/monit/monitrc" ]  ; then
       MONIT_INSTALLED=1
    fi
    

    # debian/ubuntu can install monit here
    if [ $MONIT_INSTALLED -eq 0 ] ; then

       get_linux_distribution
       if [ $LINUX_DISTRIBUTION -eq 1 ] ; then

           INSTALL_MONIT=0
           install_monit_apt_get() 
           {
               echo "" $ECHO_OUT
               echo "Do you want to install monit now using apt-get ? (y/n/h)"
               read ACCEPT
               case $ACCEPT in
                   y | Y)
                       INSTALL_MONIT=1
                       ;;
                   n | N)
                       INSTALL_MONIT=0
                       ;;
                   h | H)
                       echo "Monit can be used to monitor/restart failed processes in MySQL Cluster"
                       install_monit_apt_get
                       ;;
                   *)
                       echo "" $ECHO_OUT
                       echo "Please enter 'y' or 'n'." $ECHO_OUT
                       install_monit_apt_get
                       ;;
               esac
           }
           install_monit_apt_get


           if [ $INSTALL_MONIT -eq 1 ] ; then
               apt-get install monit
               if [ $? -ne 0 ] ; then 
                   echo "Problem installing monit. Skipping this step."
                   return 0
               fi
               MONIT_INSTALLED=1
           else
               return 0
           fi
       
       else
           echo ""
           echo "No auto-install of monit support for this Linux Distribution."
           echo "After ndbinstaller.sh has completed, you will need to download and install"
           echo "monit. Then append the upcoming monit code to /etc/monit/monitrc"
        fi

     fi
       


    echo ""        
    enter_string "Enter an email address that will receive monit alerts : " 
    MONIT_EMAIL=$ENTERED_STRING
           
    enter_string "Enter a mail server address used to send the monit alerts (default: localhost):" 
    MONIT_SNMP=$ENTERED_STRING
    if [ "$MONIT_SNMP" = "" ] ; then
       MONIT_SNMP=$LOCALHOST
    fi

    MONIT_PATCH="
#
# GENERATED BY ndbinstaller.sh
#
-set daemon  60
-set logfile $NDB_LOGS_DIR/monit.log

set mailserver $MONIT_SNMP
set mail-format { from: monit@ndbinstaller.com }
set alert $MONIT_EMAIL

check process $PROCESS with pidfile /var/lib/mysql-cluster/logs/ndb_$PID.pid
   group database
   start program = \"/etc/init.d/$PROCESS start\"
   stop program = \"/etc/init.d/$PROCESS stop\"
   $CONNECTION_TEST

" 

    case $MONIT_INSTALLED in
	1)  echo "" $ECHO_OUT
	    echo "Options:" $ECHO_OUT
	    echo "1. Patch your existing /etc/monitrc with code to monitor $PROCESS" $ECHO_OUT
	    echo "2. Print to screen the monit code, and add it manually to /etc/monitrc" $ECHO_OUT
	    echo "" $ECHO_OUT
            printf 'Please enter 1 or 2: '
	    read ACCEPT
	    case $ACCEPT in
	       1)
		    echo "" $ECHO_OUT
		    echo $MONIT_PATCH >> /etc/monit/monitrc 
		    echo "Monit code has been added to '/etc/monit/monitrc'"
		 ;;
	       *)
	       echo "" $ECHO_OUT
	       echo "Invalid choice. Printing monit code to screen." $ECHO_OUT
	       echo "$MONIT_PATCH"
	       ;;
	       esac

            ;;
        0)
	       echo "You need to append the following monit code manually to /etc/monitrc" $ECHO_OUT
	       echo "Press ENTER to print to screen the monit code."
	       echo "" $ECHO_OUT
	       clear_screen
	       echo "$MONIT_PATCH"
	       clear_screen
            ;;
        *)  exit_error "Bug: problem installing monit"
	    ;;
    esac


	    
    clear
    echo "-------------- MONIT Usage Information --------------" $ECHO_OUT
    echo ""
    echo "You can edit /etc/monit/monitrc to change how monit starts/stops/alerts"
    echo "for $PROCESS"
    echo ""
    echo "When you have finished installing the $PROCESS, you can start monit by: "
    echo "1. Make sure monit is enabled :- edit /etc/default/monit"
    echo "and set 'startup=1'"
    echo "2. Start monit using: /etc/init.d/monit start"
    echo ""
    echo "Important: monit will automatically try and start the 'ndb_mgmd' and 'ndbd'"
    echo "processes if they are not running - unless you change this in  monitrc."
    echo ""
    echo "It is recommended you install monit as a service using one of the following:"
    echo "update-rc.d monit defaults (debian/ubuntu)"
    echo "chkconfig monit on (redhat/fedora/centos)"
    echo "insserv monit on (SuSE)"
    echo ""


    clear_screen

}

###############################################################################################
# PRINT OUT INSTRUCTIONS TO USER
###############################################################################################

print_set_ndbhome()
{
# echo "For NDB/J Development: Set \$NDB_HOME evironment variable: "
# echo "bash> export NDB_HOME=$NDB_DIR"
# echo "csh> setenv NDB_HOME $NDB_DIR"
  echo "NDB_HOME=$NDB_DIR"
 echo ""
}  

print_shutdown() {
 echo "" 
 echo "$1. View the state of cluster: " 
 echo "\$NDB_HOME/scripts/$MGM_CLIENT_START -e show"
 echo "" 
 echo "$2. Shutdown the cluster, run: " 
 echo "\$NDB_HOME/scripts/$CLUSTER_SHUTDOWN"
}

print_logs() {
 echo "" 
# echo "A log of this installation is available at:" 
# echo $INSTALL_LOG 
 echo "In case of errors, examine the logs for ndbd, ndb_mgmd, mysqld processes at:" 
 echo $NDB_LOGS_DIR
 echo ""
}

start_mgmd_print()
{
  echo "=======   INSTRUCTIONS: NEXT STEPS   ======== "
  echo ""
  echo "Log on to ALL Data Node Hosts, and run ndbinstaller.sh as root." 
  echo "During install (on the data node hosts):" 
  echo "Choose Option(4) to 'Add a  Data Node to a Distributed Cluster'." 
  echo "During installation of the Data Nodes, choose the same username: '$USERNAME' !"
  echo "" 
  echo "The hostnmaes for the Data Node you have to run ndbinstaller.sh on are:" 
  count=`expr ${NUM_NODES} - 1`
  num=0
  while [ $num -le $count ] ; do
    echo -e "${NDB_HOST[$num]}" 
    num=`expr $num + 1`
  done

  echo "" 
  clear_screen
  start_cluster_print
}

start_cluster_print()
{
 echo "==== INSTRUCTIONS FOR HOW TO INITIALSE AND START THE CLUSTER ===="
 print_set_ndbhome

 if [ $ROOTUSER -eq 1 ] ; then    
     echo ""
     echo "0. Change to user '$USERNAME' to initialise/start the cluster: " 
     echo "su $USERNAME"
     echo ""
 fi

 echo "1. If you did not start the cluster, initialise and start the cluster:"
 echo "\$NDB_HOME/scripts/${INIT_START}-${NUM_NODES_TO_START}${CLUSTER_START}" 
 print_shutdown "2" "3"

 echo ""
 echo "4.Cluster has been initialised. Normal start of cluster (keeps existing data): "
 echo "\$NDB_HOME/scripts/${NORMAL_START}-${CLUSTER_START}"

 print_logs
}

start_ndbd_print()
{
 echo "======== INSTRUCTIONS FOR HOW TO INITIALSE AND START NDBDS ========"
 print_set_ndbhome

 echo "You should normally start this ndbd from the Management Server"
 echo "using the startup scripts!"
 echo ""
 echo "If you want to start the ndbd from this host, the instructions are:"
 if [ $ROOTUSER -eq 1 ] ; then    
     echo ""
     echo "0. Change to user '$USERNAME' to initialise/start the cluster: " 
     echo "su $USERNAME"
     echo ""
 fi

 echo "1. To initialise and start the ndbd (do this first), run: " 
 echo "\$NDB_HOME/scripts/${NDBD_INIT}-${NODEID}.sh" $ECHO_OUT
 echo ""

 echo "2. For normal starting of the ndbd (keeps existing data in tables), run: " 
 echo "\$NDB_HOME/scripts/${NDBD_START}-${NODEID}.sh" $ECHO_OUT

 echo "Shutdown the ndbd from the mgm client, or 'killall ndbd'" 

 print_logs
}

start_ssh_print()
{
 echo "======== INSTRUCTIONS FOR HOW TO INITIALSE THE CLUSTER ========"
 echo "You can initialise/start the cluster by:"
 echo ""
 echo "0. Logon to the Management Server host with username '$USERNAME':"
 echo "ssh $USERNAME@$MGM_HOST"
 echo ""
 echo "1. Then start and initialise the cluster (do this first), run: "      
 echo "\$NDB_HOME/scripts/${INIT_START}-${CLUSTER_START}" 
 print_shutdown "2" "3"
 echo ""
 echo "4.Cluster has been initialised. Normal start of cluster (keeps existing data): "
 echo "\$NDB_HOME/scripts/${NORMAL_START}-${CLUSTER_START}"

 clear_screen
}

start_mysqld_print()
{
 echo "===== MYSQLD STARTUP/SHUTDOWN INSTRUCTIONS  =====" $ECHO_OUT
 print_set_ndbhome
 if [ $UPDATE_SCRIPTS -eq 1 ] ; then
   echo "The MySQL Server will be started when you run the init-/start-*cluster*.sh"
   echo "scripts. The MySQL Server will stopped when you run shutdown-cluster.sh"
   echo ""
 fi
 echo "To start this mysqld individually, run: " 
 if [ $ROOTUSER -eq 1 ] ; then    
     echo "0. Change to user '$USERNAME' to initialise/start the cluster: " 
     echo "su $USERNAME"
     echo ""
 fi

 echo "\$NDB_HOME/scripts/start-mysqld-`expr $NUM_MYSQLS + 1`.sh" $ECHO_OUT

 echo "To stop this mysqld individually, run: " 
 echo "\$NDB_HOME/scripts/stop-mysqld-`expr $NUM_MYSQLS + 1`.sh" $ECHO_OUT

 echo ""
 echo "To log on to this MySQL Server, run:"
 echo "\$NDB_HOME/scripts/mysql-client-`expr $NUM_MYSQLS + 1`.sh" $ECHO_OUT
 echo ""
 print_logs
}


############################################################################################################
############################################################################################################
#                                                                                                          #
#   MAINLINE: START OF CONTROL FOR PROGRAM: FUNCTIONS CALLED FROM HERE                                     #
#                                                                                                          #
############################################################################################################
############################################################################################################

installed_progs

configure_vars

# Catch signals and clean up temp files
trap TrapBreak HUP INT TERM  

check_linux

check_userid

if [ $NON_INTERACT -eq 0 ] ; then
  splash_screen  
  display_license
  accept_license  
  clear_screen
fi

select_cpu

setup_binaries_dir

install_action

check_for_mysql_binaries

if [ $INSTALL_BINARIES -eq 1 ] ; then
  install_binaries
  setup_mysql_binary_dir
fi

if [ $INSTALL_BINARIES -eq 1 ] ; then
    build_and_move_binaries
fi

if [ $PARAM_USERNAME -eq 0 ] ; then
    setup_username
fi

setup_mysql_user_account

case $INSTALL_ACTION in
  $INSTALL_LOCALHOST)
    setup_ndb_dirs
    setup_symbolic_link
    setup_connectstring "$LOCALHOST" "$MGM_PORT"
    setup_config_ini
    setup_ndb_datadirs  
    setup_config_scripts
    setup_chown_cluster
    localhost_start_scripts
    check_for_mysql_datadirs
    INSTALL_ACTION=$INSTALL_LOCALHOST_MYSQLD
    setup_mysql_datadir
    setup_mycnf_name
    setup_my_cnf
    setup_db_ownership
    update_startup_scripts_mysqld "add_startup"

    INSTALL_ACTION=$INSTALL_LOCALHOST
    start_cluster  
    start_cluster_print
    ;;
  $INSTALL_MGM)
    setup_ndb_dirs
    setup_mycnf_name
    setup_symbolic_link
    setup_connectstring
    setup_ndbds
    setup_config_ini
    setup_config_scripts
    setup_chown_cluster
    setup_ssh_on_mgmd
    mgm_start_scripts
    if [ $ROOTUSER -eq 1 ] ; then    
       install_services
    fi

    echo "-------------- Optional Installation of MySQL Server --------------" $ECHO_OUT
    add_mysqld() 
    {
	 echo "" $ECHO_OUT
         printf 'Do you want to add a MySQL Server (mysqld) to the cluster?: (y/n) '
	 read ACCEPT
	 case $ACCEPT in
	    y | Y)
	        ADD_MYSQLD=1
		INSTALL_ACTION=$INSTALL_LOCALHOST_MYSQLD
		;;
            n | N)
	        ADD_MYSQLD=0
		;;
            *)
	      echo "" $ECHO_OUT
              echo "Please enter 'y' or 'n'." $ECHO_OUT
	      add_mysqld
         esac
    }
    if [ $ADD_MYSQLD -eq 0 ]  && [ $NON_INTERACT -eq 0 ] ; then
	add_mysqld
    else
        INSTALL_ACTION=$INSTALL_LOCALHOST_MYSQLD
    fi
    clear_screen
    if [ $ADD_MYSQLD -eq 1 ] ; then
      check_for_mysql_datadirs
      setup_mysql_datadir
      setup_mycnf_name
      setup_my_cnf
      setup_db_ownership
      update_startup_scripts_mysqld "add_startup"
    fi

    INSTALL_ACTION=$INSTALL_MGM
    if [ $START_WITH_SSH -eq 1 ] ; then
	start_ssh_print
    fi
    start_mgmd_print

    ;;
  $INSTALL_NDB)
    NUM_NODES=1
    setup_ndb_dirs
    setup_symbolic_link
    setup_ndb_datadirs  
    setup_chown_cluster
    setup_connectstring
    setup_ssh_on_ndbd
    ndbd_start_scripts
    if [ $ROOTUSER -eq 1 ] ; then    
       install_services
    fi
    if [ $START_WITH_SSH -eq 1 ] ; then
	start_ssh_print
    else
#	start_ndbds
#	start_ndbd_print
	start_cluster_print
    fi
    ;;
  $INSTALL_MYSQLD)
    check_for_mysql_datadirs
    setup_mysql_datadir
    setup_symbolic_link
    setup_mycnf_name
    setup_connectstring
    setup_mysqld_hostname
    setup_my_cnf
    setup_db_ownership
    update_startup_scripts_mysqld  
    setup_chown_cluster
    start_mysqld_print
    ;;
  $INSTALL_LOCALHOST_MYSQLD)
    check_for_mysql_datadirs
    setup_mysql_datadir
    setup_mycnf_name
    setup_connectstring $LOCALHOST $DEFAULT_MGM_PORT
    setup_my_cnf
    setup_db_ownership
    update_startup_scripts_mysqld
    start_mysqld_print
    ;;
  $INSTALL_ANOTHER_MYSQLD_LOCALHOST)
    check_for_mysql_datadirs
    setup_mysql_datadir
    setup_mycnf_name
    setup_my_cnf
    setup_db_ownership
    update_startup_scripts_mysqld  
    setup_chown_cluster
    start_mysqld_print
    ;;
  $INSTALL_ANOTHER_MYSQLD)
    check_for_mysql_datadirs
    setup_mysql_datadir
    setup_mycnf_name
    setup_connectstring $MGM_HOST $MGM_PORT
    setup_mysqld_hostname
    setup_my_cnf
    setup_db_ownership
    update_startup_scripts_mysqld  
    setup_chown_cluster
#    start_mysqlds
    start_mysqld_print
    ;;
   *)
    echo "" 
    echo "Invalid install action." 
    exit_error
esac

echo "java.library.path should be set to $MYSQL_BINARIES_DIR/lib"
exit 0
