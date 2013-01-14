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
       :user => node[:ndb][:user],
       :ndb_dir => node[:ndb][:base_dir],
       :mysql_dir => node[:mysql][:base_dir],
       :ndb_mysql_dir => node[:ndb][:mysql_server_dir],
       :ndb_mysql_data_dir => node[:ndb][:mysql_data_dir],
       :connect_string => node[:ndb][:connect_string],
       :mysql_port => node[:ndb][:mysql_port],
       :mysql_socket => node[:ndb][:mysql_socket],
       :node_id => node[:mysql][:id]
    })
  end
end 

ndb_mysql_start "init" do
  action :nothing
end

template "mysql.cnf" do
  owner node[:ndb][:user]
  group node[:ndb][:group]
  path "#{node[:ndb][:base_dir]}/my.cnf"
  source "my.cnf.erb"
  mode "0644"
  variables({
              :ndb_dir => node[:ndb][:base_dir],
              :mysql_dir => node[:mysql][:base_dir],
              :connect_string => node[:ndb][:connect_string]
            })
  notifies :restart, resources(:service => "mysqld")
end


template "/etc/init.d/mysqld" do
  source "mysqld.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0755
  variables({
              :ndb_dir => node[:ndb][:base_dir],
              :mysql_dir => node[:mysql][:base_dir],
              :connect_string => node[:ndb][:connect_string]
            })
#  notifies :initialize, resources(:ndb_mysql_start => "init"), :immediately
  notifies :enable, resources(:service => "mysqld")
end

  bash 'mysql_install_db' do
    user "#{node[:ndb][:user]}"
    code <<-EOF
    cd #{node[:mysql][:base_dir]}
    # --force causes mysql_install_db to run even if DNS does not work. In that case, grant table entries that normally use host names will use IP addresses.
    #{node[:mysql][:base_dir]}/scripts/mysql_install_db --basedir=#{node[:mysql][:base_dir]} --defaults-file=#{node[:ndb][:base_dir]}/my.cnf --force 
    EOF
    not_if { ::File.exists?( "#{node[:ndb][:mysql_server_dir]}/mysql" ) }
  end


service "mysqld" do
  supports :restart => true, :stop => true, :start => true
  action :start, :immediately
end

