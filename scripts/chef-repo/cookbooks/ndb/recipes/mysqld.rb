include_recipe "ndb"

libaio1="libaio1_0.3.109-2ubuntu1_amd64.deb"
cached_libaio1 = "#{Chef::Config[:file_cache_path]}/#{libaio1}"
Chef::Log.info "Installing libaio1 to #{cached_libaio1}"

cookbook_file "#{cached_libaio1}" do
  source "#{libaio1}"
  owner node[:ndb][:user]
  group node[:ndb][:group]
  mode "0755"
  action :create_if_missing
end

package "#{libaio1}" do
  provider Chef::Provider::Package::Dpkg
  source "#{cached_libaio1}"
  action :install
end

directory node[:ndb][:mysql_server_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:group]
  mode "0755"
  action :create
  recursive true  
end

for script in node[:mysql][:scripts]
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner node[:ndb][:user]
    group node[:ndb][:group]
    mode 0774
    variables({
                :node_id => node[:mysql][:id]
              })
  end
end 

service "mysqld" do
  supports :restart => true, :stop => true, :start => true
  action :nothing
end

template "mysql.cnf" do
  owner node[:ndb][:user]
  group node[:ndb][:group]
  path "#{node[:ndb][:root_dir]}/my.cnf"
  source "my.cnf.erb"
  mode "0644"
  notifies :restart, resources(:service => "mysqld")
end

bash 'mysql_install_db' do
  user "#{node[:ndb][:user]}"
  code <<-EOF
    # --force causes mysql_install_db to run even if DNS does not work. In that case, grant table entries that normally use host names will use IP addresses.
    #{node[:mysql][:base_dir]}/scripts/mysql_install_db --basedir=#{node[:mysql][:base_dir]} --defaults-file=#{node[:ndb][:root_dir]}/my.cnf --force 
    touch #{node[:ndb][:mysql_server_dir]}/.installed
    EOF
  not_if { ::File.exists?( "#{node[:ndb][:mysql_server_dir]}/.installed" ) }
end

ndb_mysql_start "install" do
  action :nothing
end


template "/etc/init.d/mysqld" do
  source "mysqld.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0755
  notifies :enable, resources(:service => "mysqld")
  notifies :restart, resources(:service => "mysqld"), :immediately
  notifies :install_distributed_privileges, resources(:ndb_mysql_start => "install"), :immediately
end
