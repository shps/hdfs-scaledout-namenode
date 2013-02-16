#require 'Dir'

# Default values for configuration parameters
default[:kthfs][:user] = "root"
default[:kthfs][:base_dir] = "/var/lib/kthfsagent"
#default[:kthfs][:rest_url] = "https://10.0.2.15:8181/KTHFSDashboard/rest/agent/keep-alive"
default[:kthfs][:rest_url] = "http://#{node[:ipaddress]}:8080/KTHFSDashboard/rest/agent/keep-alive"
default[:kthfs][:port] = 8090
default[:kthfs][:heartbeat_interval] = 10
default[:kthfs][:watch_interval] = 2
default[:kthfs][:rest_user] = "kthfsagent@sics.se"
default[:kthfs][:rest_password] = "kthfsagent"
default[:kthfs][:pid_file] =  "#{Dir.tmpdir}/kthfs-agent.pid"
default[:kthfs][:logging_level] = "INFO"
default[:kthfs][:max_log_size] = "10000000"

default[:kthfs][:agent_user] = "kthfsagent@sics.se"
default[:kthfs][:agent_password] = "kthfsagent"
default[:kthfs][:certificate_file] = "server.pem"
default[:kthfs][:instance] = "hdfs1"

