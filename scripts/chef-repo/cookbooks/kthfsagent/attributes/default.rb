#require 'Dir'

# Default values for configuration parameters
default[:kthfs][:user] = "root"
default[:kthfs][:base_dir] = "/var/lib/kthfsagent"
default[:kthfs][:rest_url] = "http://cloud16.sics.se:8080/KTHFSDashboard/rest/agent/keep-alive"
default[:kthfs][:port] = 8090
default[:kthfs][:heartbeat_interval] = 10
default[:kthfs][:watch_interval] = 2
default[:kthfs][:rest_user] = "kthfs"
default[:kthfs][:rest_password] = "kthfs"
default[:kthfs][:pid_file] =  "#{Dir.tmpdir}/kthfs-agent.pid"
