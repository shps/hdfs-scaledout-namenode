# Stop all the services

bash 'kill_running_services' do
user "root"
ignore_failure :true
code <<-EOF
 killall -9 ndb_mgmd
 killall -9 ndbmtd
 killall -9 mysqld
 killall -9 memcached
EOF
end

# Remove all services

file "/etc/init.d/ndb_mgmd" do
  action :delete
  ignore_failure :true
end

file "/etc/init.d/ndbd" do
  action :delete
  ignore_failure :true
end

file "/etc/init.d/mysqld" do
  action :delete
  ignore_failure :true
end

file "/etc/init.d/memcached" do
  action :delete
  ignore_failure :true
end


# Remove the MySQL binaries and MySQL Cluster data directories

directory node[:ndb][:root_dir] do
  recursive true
  action :delete
  ignore_failure :true
end

# TODO - don't know if wildcards are supported for deleting files/directories
#directory "#{node[:mysql][:base_dir]}*" do

directory node[:mysql][:version_dir] do
  recursive true
  action :delete
  ignore_failure :true
end

link node[:mysql][:base_dir] do
  action :delete
  ignore_failure :true
end
