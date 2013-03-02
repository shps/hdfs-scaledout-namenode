include_recipe "ndb::kthfs"

Chef::Log.info "Trying to infer the mgmd ID by examining the local IP. If it matches the config.ini file, then we have our node."
found_id = -1
id = 1
for mgmd in node[:ndb][:mgmd][:addrs]
  if node[:ndb][:private_ip].eql? mgmd
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

ndb_kthfs_services node[:ndb][:kthfs_services] do
 node_id found_id
 action :install_mgmd
end
