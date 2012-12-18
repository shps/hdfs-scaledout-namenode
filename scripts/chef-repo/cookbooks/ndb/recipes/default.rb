include_recipe "kthfsagent"

inifile_url = node[:ndb][:inifile_gem]
inifile_filename = File.basename(inifile_url)
cached_inifile_filename = "#{Chef::Config[:file_cache_path]}/#{inifile_filename}"

# Chef::Log.info "Downloading #{cached_inifile_filename} from #{inifile_url} "

remote_file cached_inifile_filename do
    source inifile_url
    mode "0755"
    action :create_if_missing
end

gem_package "inifile" do
 source cached_inifile_filename
 action :install
end

user node[:ndb][:user] do
  action :create
  system true
  shell "/bin/bash"
end

directory node[:ndb][:scripts_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end

directory node[:ndb][:log_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end

directory node[:mysql][:base_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  recursive true
end

template "#{node[:ndb][:base_dir]}/config.ini" do
  source "config.ini.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0644
  variables({:cores => node[:cpu][:total]})
#    notifies :restart, resources(:service => "ndbd")
end
# 
package_url = "#{node[:ndb][:package_url]}/#{node[:ndb][:package_src]}"
Chef::Log.info "Downloading mysql cluster binaries from #{package_url}"
base_package_filename =  File.basename(node[:ndb][:package_url])
Chef::Log.info "Into file #{base_package_filename}"
base_package_dirname =  File.basename(base_package_filename, ".tar.gz")
cached_package_filename = "#{Chef::Config[:file_cache_path]}/#{base_package_filename}"
Chef::Log.info "You should find it in:  #{cached_package_filename}"

remote_file cached_package_filename do
  source package_url
  mode "0600"
  action :create_if_missing
end

bash "unpack_mysql_cluster" do
    code <<-EOF
cd #{Chef::Config[:file_cache_path]}
tar -xzf #{base_package_filename}
#rm #{base_package_filename}
cp -r #{base_package_dirname}/* #{node[:mysql][:base_dir]}
EOF
  not_if { ::File.exists?( "#{node[:mysql][:base_dir]}/bin/ndbd" ) }
end
