name             "Ndb (MySQL Cluster)"
maintainer       "Jim Dowling"
maintainer_email "jdowling@kth.se"
license          "GPL 2.0"

description      "Installs/Configures NDB (MySQL Cluster)"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))

version          "1.0"

recipe            "ndb::mgmd", "Installs a MySQL Cluster management server (ndb_mgmd)"
recipe            "ndb::ndbd", "Installs a MySQL Cluster data node (ndbd)"
recipe            "ndb::mysqld", "Installs a MySQL Server connected to the MySQL Cluster (mysqld)"
recipe            "ndb::memcached", "Installs a memcached Server connected to the MySQL Cluster (memcached)"

recipe            "ndb::mgmd-kthfs", "Configures the kthfs agent for the NDB management server"
recipe            "ndb::ndbd-kthfs", "Configures the kthfs agent for the NDB data node"
recipe            "ndb::mysqld-kthfs", "Configures the kthfs agent for the NDB MySQL Server"
recipe            "ndb::memcached-kthfs", "Configures the kthfs agent for the NDB memcached Server"


%w{ ubuntu debian }.each do |os|
  supports os
end

attribute "ndb/mgm_server/addrs",
:display_name => "Mgm server IP addresses",
:description => "List of IP addresses of ndb_mgmd processes (mgm servers)",
:type => 'array',
:default => ""

attribute "ndb/ndbd/addrs",
:display_name => "Data node IP addresses",
:description => "List of IP addresses of ndbd processes (data nodes)",
:type => 'array',
:default => ""

attribute "ndb/ndbapi/addrs",
:display_name => "API client IP addresses",
:description => "List of IP addresses of mysqld, clusterj, and memcached processes (mysql servers, java client apps, memcached servers).",
:type => 'array',
:default => ""

attribute "ndb/my_ip",
:display_name => "IP address",
:description => "IP address used by this node",
:type => 'string',
:default => ""


attribute "mysql/password",
:display_name => "MySQL Server Password for 'kthfs' user",
:description => "Password for the 'kthfs' user",
:type => 'string',
:default => "kthfs"

attribute "ndb/data_memory",
:display_name => "Data memory",
:description => "Data memory for each MySQL Cluster Data Node",
:type => 'string',
:default => "80"

attribute "ndb/index_memory",
:display_name => "Index memory",
:description => "Index memory for each MySQL Cluster Data Node",
:type => 'string',
:default => "20"

attribute "ndb/num_replicas",
:display_name => "Num Replicas",
:description => "Num of replicas of the MySQL Cluster Data Nodes",
:type => 'string',
:default => "2"

attribute "ndb/private_ip",
:display_name => "Private ip address",
:description => "Private ip address used by MySQL Cluster",
:type => 'string',
:default => ""

attribute "ndb/num_ndb_slots_per_client",
:display_name => "Number ndbapi slots per client",
:description => "Number of ndbapi slots used by MySQL Server, clusterj, and memcached",
:type => 'string',
:default => "3"

attribute "memcached/mem_size",
:display_name => "Memcached data memory size",
:description => "Memcached data memory size",
:type => 'string',
:default => "80"

attribute "memcached/options",
:display_name => "Memcached options",
:description => "Memcached options",
:type => 'string',
:default => ""

