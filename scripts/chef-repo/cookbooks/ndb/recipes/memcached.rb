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
