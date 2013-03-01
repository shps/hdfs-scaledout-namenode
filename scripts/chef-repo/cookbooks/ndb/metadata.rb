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

# attribute "ndb/mysql_server/addrs",
# :display_name => "MySQL server IP addresses in MySQL Cluster",
# :description => "List of IP addresses of mysqld processes (mysql servers)",
# :type => 'array',
# :default => ""

# attribute "ndb/memcached/addrs",
# :display_name => "Memcached server IP addresses in MySQL Cluster",
# :description => "List of IP addresses of memcached processes (memcached servers)",
# :type => 'array',
# :default => ""

# attribute "ndb/clusterj/addrs",
# :display_name => "Cluster/J IP addresses in MySQL Cluster",
# :description => "List of IP addresses of cluster/j processes (ndb clients)",
# :type => 'array',
# :default => ""

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
