include_recipe "ndb::kthfs"
#include "kthfsagent"
include "collectd"

Chef::Log.info "Trying to infer the mysqld ID by examining the local IP. If it matches the config.ini file, then we have our node."
found_id = -1
id = 1
for api in node[:ndb][:ndbapi][:addrs]
  if node[:ndb][:private_ip].eql? api
    Chef::Log.info "Found matching IP address in the list of nodes: #{api} . ID= #{id}"
    @found = true
    found_id = id
  end
  id += 1
end 

Chef::Log.info "ID IS: #{id}"

if @found != true
  Chef::Log.fatal "Could not find matching IP address is list of data nodes."
end

ndb_kthfs_services "#{node[:ndb][:kthfs_services]}" do
 node_id found_id
 action :install_mysqld
end

# TODO -COLLECTD - conf file update
# collectd_plugin "dbi" do
#   options :data_dir=>"/var/lib/collectd/rrd"
# end
 
