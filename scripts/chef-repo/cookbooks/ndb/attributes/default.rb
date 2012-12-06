# default[:ndb][:dir]       = "/etc/ndb.ini"
# default[:ndb][:data_dir]  = "/var/lib/mysql-cluster/ndb"
# default[:ndb][:log_dir]   = "/var/log/ndb"
# one of: debug, verbose, notice, warning

default[:ndb][:loglevel]  = "notice"
default[:ndb][:mgmServer] = "cloud11"
default[:ndb][:user]      = "root"
default[:ndb][:port]      = 1186
default[:ndb][:packageSrc] = "from/http://cdn.mysql.com/"
default[:ndb][:packageUrl] = "http://dev.mysql.com/get/Downloads/MySQL-Cluster-7.2/mysql-cluster-gpl-7.2.8-linux2.6-x86_64.tar.gz"


# percentage of total memory used by Data Nodes
default[:ndb][:memory] = 80
default[:ndb][:dataNodes] = %w{ cloud8 cloud9 cloud12 cloud13 }
default[:ndb][:mgmServers] = %w{ cloud11 }


default[:ndb][:version] = "7.2.9"
default[:ndb][:base_dir] = "/var/lib/mysql-cluster/ndb-" + default[:ndb][:version]
default[:mysql][:base_dir] = "/usr/local/mysql-" + default[:ndb][:version]
