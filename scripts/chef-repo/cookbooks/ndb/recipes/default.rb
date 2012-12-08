
package_url = "#{node[:ndb][:package_url]}/#{node[:ndb][:package_src]}"

base_package_filename =  File.basename(node[:ndb][:package_url])
base_package_dirname =  File.basename(base_package_filename, ".tar.gz")
cached_package_filename = "#{Chef::Config[:file_cache_path]}/#{base_package_filename}"

remote_file cached_package_filename do
  source package_url
  mode "0600"
  action :create_if_missing

end

directory "#{node[:ndb][:base_dir]}/scripts" do
  owner "root"
  group "root"
  mode "755"
  recursive true
end

directory node[:mysql][:base_dir] do
  owner "root"
  group "root"
  mode "755"
  recursive true
end


template "#{node[:ndb][:base_dir]}/config.ini" do
  source "config.ini.erb"
  owner "root"
  group "root"
  mode 0644
  variables({:cores => node[:cpu][:total]})
#    notifies :restart, resources(:service => "ndbd")
end

bash "unpack_mysql_cluster" do
    code <<-EOF
cd #{Chef::Config[:file_cache_path]}
tar -xzf #{base_package_filename}
#rm #{base_package_filename}
cp -r #{base_package_dirname}/* #{node[:mysql][:base_dir]}
EOF
  not_if { ::File.exists?( "#{mysql_dir}/bin/mysqld" ) }
end

service "ndb" do
  provider Chef::Provider::Service::Upstart
  subscribes :restart, resources(:unpack_mysql_cluster => "ndb")
  supports :restart => true, :start => true, :stop => true
end

template "ndbd.upstart.conf" do
  path "/etc/init/ndb.conf"
  source "ndb.upstart.conf.erb"
  owner "root"
  group "root"
  mode "0644"
  notifies :restart, resources(:service => "redis")
end

service "ndb" do
  action [:enable, :start]
#  action :start, :immediately
end
