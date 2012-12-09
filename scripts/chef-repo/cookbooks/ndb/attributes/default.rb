# one of: debug, verbose, notice, warning

version="7"
majorVersion="2"
minorVersion="9"
versionStr= "7.2.9"

default[:ndb][:loglevel]  = "notice"
default[:ndb][:mgm_server] = "cloud11"
default[:ndb][:user]      = "root"
default[:ndb][:port]      = "1186"
default[:ndb][:package_src] = "from/http://cdn.mysql.com/"
default[:ndb][:package_url] = "http://dev.mysql.com/get/Downloads/MySQL-Cluster-#{version}.#{majorVersion}/mysql-cluster-gpl-#{versionStr}-linux2.6-x86_64.tar.gz"
default[:ndb][:connect_string] = "#{default[:ndb][:mgm_server]}:#{ default[:ndb][:port]}"


 # percentage of total memory used by Data Nodes
default[:ndb][:data_memory] = 500
default[:ndb][:index_memory] = 100
default[:ndb][:data_nodes] = %w{ cloud8 cloud9 cloud12 cloud13 10.0.2.15 }

default[:mgm][:scripts] = %w{ backup-start.sh enter-singleuser-mode.sh mgm-client.sh mgm-server-start.sh mgm-server-stop.sh mgm-server-restart.sh cluster-shutdown.sh cluster-start.sh exit-singleuser-mode.sh }
default[:ndb][:scripts] = %w{ ndbd-start.sh ndbd-init.sh ndbd-stop.sh ndbd-restart.sh }
default[:mysql][:scripts] = %w{ mysql-server-start.sh mysql-server-stop.sh mysql-server-restart.sh mysql-client.sh }

default[:ndb][:version] = #{versionStr}
default[:ndb][:base_dir] = "/var/lib/mysql-cluster/ndb-#{versionStr}"
default[:ndb][:log_dir] = "#{default[:ndb][:base_dir]}" + "/log"
default[:ndb][:data_dir] = "#{default[:ndb][:base_dir]}" + "/ndb_data"
default[:ndb][:scripts_dir] = "#{default[:ndb][:base_dir]}" + "/scripts"
default[:mysql][:base_dir] = "/usr/local/mysql-#{versionStr}"

default[:ndb][:mysql_server_dir] = "#{default[:ndb][:base_dir]}" + "/mysql"
default[:ndb][:mysql_data_dir] = "#{default[:ndb][:mysql_server_dir]}" + "/data"
default[:ndb][:mysql_port] = "3306"
default[:ndb][:mysql_socket] = "/tmp/mysql.sock"
