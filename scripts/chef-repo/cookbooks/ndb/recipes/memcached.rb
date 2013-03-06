include_recipe "ndb"
Chef::Log.info "Memcached for NDB"

theResource="memcached-installer"
theService="memcached"

service "#{theService}" do
  supports :restart => true, :stop => true, :start => true
  action [ :nothing ]
end

ndb_mysql_start "#{theResource}" do
  action :nothing
end

case node[:platform_family]
when "debian" # also includes ubuntu in platform_family
  found_id = -1
  id = 1
  for mysqld in node[:ndb][:ndbapi][:addrs]
    if node[:ndb][:private_ip].eql? mysqld
      Chef::Log.info "Found matching IP address in the list of data nodes: #{mysqld} . ID= #{id}"
      @found = true
      found_id = id
    end
    id += 1
  end 
  Chef::Log.info "ID IS: #{id}"
  if @found != true
    Chef::Log.fatal "Could not find matching IP address is list of data nodes."
  end
end

for script in node[:memcached][:scripts]
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner node[:ndb][:user]
    group node[:ndb][:group]
    mode 0774
    variables({
                :node_id => found_id
              })
  end
end 

template "/etc/init.d/#{theService}" do
  source "#{theService}.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0755
  variables({
              :ndb_dir => node[:ndb][:base_dir],
              :mysql_dir => node[:mysql][:base_dir],
              :connect_string => node[:ndb][:connect_string]
            })
  notifies :install_memcached, resources(:ndb_mysql_start => "#{theResource}"), :immediately
  notifies :enable, resources(:service => "#{theService}")
  notifies :restart, resources(:service => "#{theService}")
end
