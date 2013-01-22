include_recipe "ndb::kthfs"

Chef::Log.info "Trying to infer the ndbd ID by examining the local IP. If it matches the config.ini file, then we have our node."

found_id = -1
id = 1
# if no default IP is set, then look around for the IP 
if node.attribute?('ipaddress') != true
  Chef::Log.warn "Node has no default IP address specified!"
  # just guess what the IP is here
  ipaddress_eth1 = host["network"]["interfaces"]["eth1"]["addresses"].select { |address, data| data["family"] == "inet" }[0][0]
  ipaddress_eth0 = host["network"]["interfaces"]["eth0"]["addresses"].select { |address, data| data["family"] == "inet" }[0][0]
  for ndbd in node[:ndb][:data_nodes]
    Chef::Log.info "Testing IP address: #{ndbd}"
    if ipaddress_eth1.eql? ndbd
      @found = true
      found_id = id
    end
    if ipaddress_eth0.eql? ndbd
      @found = true
      found_id = id
    end
    id += 1
  end 
else
# default IP is set, here
  for ndbd in node[:ndb][:data_nodes]
    Chef::Log.info "Testing IP address: #{ndbd}"
    if node['ipaddress'].eql? ndbd
      Chef::Log.info "Found matching IP address in the list of data nodes: #{ndbd} . ID= #{id}"
      @found = true
      found_id = id
    end
    id += 1
  end 
end
  Chef::Log.info "ID IS: #{id}"

  if @found != true
    Chef::Log.info "Could not find matching IP address is list of data nodes."
  end


ndb_kthfs_services "#{node[:ndb][:kthfs_services]}" do
 node_id found_id
 action :install_ndbd
end
