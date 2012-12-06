
package_url = "#{node['ndb']['packageUrl']}/#{node[:ndb]['packageSrc']}"

base_package_filename =  File.basename(node['ndb']['packageUrl'])
base_package_dirname =  File.basename(base_package_filename, ".tar.gz")
cached_package_filename = "#{Chef::Config[:file_cache_path]}/#{base_package_filename}"

remote_file cached_package_filename do
  source package_url
  mode "0600"
  action :create_if_missing

end

ndb_dir = "#{node[:ndb][:base_dir]}"

directory ndb_dir do
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


template "#{ndb_dir}/config.ini" do
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
 # not_if { ::File.exists?( #{mysql_dir} ) }
end

