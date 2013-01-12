#include_recipe "kthfsagent"

user node[:ndb][:user] do
  action :create
  system true
  shell "/bin/bash"
end

directory node[:ndb][:version_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end


link node[:ndb][:base_dir] do
  action :delete
  only_if "test -L #{node[:ndb][:base_dir]}"
end

link node[:ndb][:base_dir] do
  to node[:ndb][:version_dir]
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

directory node[:mysql][:version_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end


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
cp -r #{base_package_dirname}/* #{node[:mysql][:version_dir]}
test -f #{node[:mysql][:base_dir]} && rm -rf #{node[:mysql][:base_dir]}
EOF
  not_if { ::File.exists?( "#{node[:mysql][:version_dir]}/bin/ndbd" ) }
end

# Bug - this prevent link from being created
# link node[:mysql][:base_dir] do
#   action :delete
#   only_if "test -L #{node[:mysql][:base_dir]}"
# end

link node[:mysql][:base_dir] do
  to node[:mysql][:version_dir]
end


# This doesn't work yet.
# ark 'mysql' do
#    version "#{node[:ndb][:version]}"
#    url "#{package_url}"
#    path "/usr/local"
#    home_dir "/usr/local/mysql"
#    append_env_path true
#    owner node[:ndb][:user] 
# end
