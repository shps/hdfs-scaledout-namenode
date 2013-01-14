Chef::Log.info "Starting memcached service"

ndb_mysql_start "install" do
  action :install_memcached
end

service "memcached" do
  supports :restart => true, :stop => true, :start => true
  action [ :start ]
end
