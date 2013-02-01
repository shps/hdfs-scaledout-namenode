default[:ndb][:ndbd][:run_state]  = :stop
default[:ndb][:mysql_server][:run_state]  = :stop
default[:ndb][:mgm_server][:run_state]  = :stop



version="7"
majorVersion="2"
minorVersion="10"
versionStr= "#{version}.#{majorVersion}.#{minorVersion}"

default[:ndb][:loglevel]   = "notice"
default[:ndb][:mgm_server][:addr] = "10.0.2.15"
default[:ndb][:user]       = "root"
default[:ndb][:group]      = "root"
default[:ndb][:port]       = "1186"
default[:ndb][:package_src] = "from/http://cdn.mysql.com/"
default[:ndb][:package_url] = "http://dev.mysql.com/get/Downloads/MySQL-Cluster-#{version}.#{majorVersion}/mysql-cluster-gpl-#{versionStr}-linux2.6-x86_64.tar.gz"
default[:ndb][:connect_string] = "#{default[:ndb][:mgm_server][:addr]}:#{ default[:ndb][:port]}"
default[:ndb][:data_memory] = 50
default[:ndb][:index_memory] = 10
default[:ndb][:data_nodes] = %w{ 10.0.2.15 } # cloud8.sics.se cloud9.sics.se cloud16.sics.se 
default[:ndb][:num_replicas] = 1
default[:ndb][:num_client_slots] = 10

default[:mgm][:scripts] = %w{ enter-singleuser-mode.sh mgm-client.sh mgm-server-start.sh mgm-server-stop.sh mgm-server-restart.sh cluster-shutdown.sh  exit-singleuser-mode.sh }
default[:ndb][:scripts] = %w{ backup-start.sh backup-restore.sh ndbd-start.sh ndbd-init.sh ndbd-stop.sh ndbd-restart.sh }
default[:mysql][:scripts] = %w{ get-mysql-socket.sh get-mysql-port.sh mysql-server-start.sh mysql-server-stop.sh mysql-server-restart.sh mysql-client.sh memcached-start.sh memcached-stop.sh memcached-restart.sh }

default[:ndb][:version] = #{versionStr}
default[:ndb][:root_dir] = "/var/lib/mysql-cluster"
default[:ndb][:log_dir] = "#{default[:ndb][:root_dir]}" + "/log"
default[:ndb][:data_dir] = "#{default[:ndb][:root_dir]}" + "/ndb_data"
default[:ndb][:version_dir] = "#{default[:ndb][:root_dir]}" + "/ndb-#{versionStr}"
default[:ndb][:base_dir] = "#{default[:ndb][:root_dir]}" + "/ndb"

default[:ndb][:scripts_dir] = "#{default[:ndb][:base_dir]}" + "/scripts"
default[:ndb][:mgm_dir] = "#{default[:ndb][:root_dir]}" + "/mgmd"

default[:ndb][:mysql_server_dir] = "#{default[:ndb][:base_dir]}" + "/mysql"
default[:ndb][:mysql_port] = "3306"
default[:ndb][:mysql_socket] = "/tmp/mysql.sock"

default[:ndb][:num_clients]  = "20"

default[:ndb][:kthfs_services] = "/var/lib/kthfsagent/services"
default[:ndb][:instance] = "hdfs1"

default[:ndb][:wait_startup] = 300

default[:mysql][:base_dir] = "/usr/local/mysql"
default[:mysql][:version_dir] = "#{default[:mysql][:base_dir]}" + "-#{versionStr}"
default[:mysql][:user]      = "kthfs"
default[:mysql][:password]  = "kthfs"


default[:collectd][:conf] = "/etc/collectd/collectd.conf"


default[:mgm][:id] = 49
default[:mysql][:id] = 50
default[:memcached][:id] = 51

# Size in MB of memcached cache
default[:memcached][:mem_size] = 64
# See examples here for configuration: http://dev.mysql.com/doc/ndbapi/en/ndbmemcache-configuration.html
# options examples: ";dev=role"   or ";dev=role;S:c4,g1,t1" or ";S:c0,g1,t1" ";role=db-only"
default[:memcached][:options] = ";role=ndb-caching;usec_rtt=250;max_tps=100000;m=#{default[:memcached][:mem_size]}"
