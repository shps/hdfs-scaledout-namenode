include_recipe "ndb"
Chef::Log.info "Memcached for NDB"

service "memcached" do
  supports :restart => true, :stop => true, :start => true
  action [ :nothing ]
end

template "/etc/init.d/memcached" do
  source "memcached.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0755
  variables({
              :ndb_dir => node[:ndb][:base_dir],
              :mysql_dir => node[:mysql][:base_dir],
              :connect_string => node[:ndb][:connect_string]
            })
  notifies :enable, resources(:service => "memcached")
end
