# one of: debug, verbose, notice, warning

version=7
majorVersion=2
minorVersion=9
versionStr="#{version}.#{majorVersion}.#{minorVersion}"

default[:ndb][:loglevel]  = "notice"
default[:ndb][:mgm_server] = "cloud11"
default[:ndb][:user]      = "root"
default[:ndb][:port]      = 1186
default[:ndb][:package_src] = "from/http://cdn.mysql.com/"
default[:ndb][:package_url] = "http://dev.mysql.com/get/Downloads/MySQL-Cluster-#{version}.#{majorVersion}/mysql-cluster-gpl-#{versionStr}-linux2.6-x86_64.tar.gz"
default[:ndb][:connect_string] = "#{default[:ndb][:mgm_server]}:#{default[:ndb][:port]}"


 # percentage of total memory used by Data Nodes
default[:ndb][:memory] = 80
default[:ndb][:data_nodes] = %w{ cloud8 cloud9 cloud12 cloud13 }

default[:mgm][:scripts] = %w{ mgm-client.sh rolling-restart.sh init-cluster.sh mgm-server-start.sh mgm-server-restart.sh shutdown-cluster.sh copy-backup-to-mgm-server.sh mgm-server-stop.sh start-backup.sh enter-singleuser-mode.sh start-cluster-with-recovery.sh exit-singleuser-mode.sh  memory-usage.sh ndb_mgm_service ndb.upstart.conf }
default[:ndb][:scripts] = %w{ start-noinit-ndbd.sh init-start-ndbd.sh stop-ndbd.sh restart-ndbd.sh ndb_service ndb.upstart.conf }
default[:mysql][:scripts] = %w{ start-mysql-server.sh stop-mysql-server.sh mysql-client.sh }

default[:ndb][:version] = #{versionStr}
default[:ndb][:base_dir] = "/var/lib/mysql-cluster/ndb-" + #{versionStr}
default[:ndb][:log_dir] = "#{default[:ndb][:base_dir]}/logs"
default[:ndb][:data_dir] = "#{default[:ndb][:base_dir]}/ndb_data"
default[:mysql][:base_dir] = "/usr/local/mysql-" + #{versionStr}

default[:ndb][:mysql_server_dir] = "#{default[:ndb][:base_dir]}/mysql_server"
default[:ndb][:mysql_data_dir] = "#{default[:ndb][:mysql_server_dir]}/data"
default[:ndb][:mysql_port] = 3306
default[:ndb][:mysql_socket] = "/tmp/mysql.sock"
