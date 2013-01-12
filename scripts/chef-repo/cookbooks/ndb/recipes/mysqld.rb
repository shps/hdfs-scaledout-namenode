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

service "mysqld" do
  supports :restart => true, :stop => true, :start => true
  action [ :nothing ]
end


template "mysql.cnf" do
  path "#{node[:ndb][:base_dir]}/my.cnf"
  source "my.cnf.erb"
  owner "root"
  group "root"
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
  notifies :enable, resources(:service => "mysqld")
  notifies :start, resources(:service => "mysqld"), :immediately
end



for script in node[:mysql][:scripts]
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner "root"
    group "root"
    mode 0774
    variables({
       :user => node[:ndb][:user],
       :ndb_dir => node[:ndb][:base_dir],
       :mysql_dir => node[:mysql][:base_dir],
       :ndb_mysql_dir => node[:ndb][:mysql_server_dir],
       :ndb_mysql_data_dir => node[:ndb][:mysql_data_dir],
       :connect_string => node[:ndb][:connect_string],
       :mysql_port => node[:ndb][:mysql_port],
       :mysql_socket => node[:ndb][:mysql_socket]
    })
  end
end 


# mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql
# SELECT ROUTINE_NAME, ROUTINE_SCHEMA, ROUTINE_TYPE 
# ->     FROM INFORMATION_SCHEMA.ROUTINES 
# ->     WHERE ROUTINE_NAME LIKE 'mysql_cluster%'
# ->     ORDER BY ROUTINE_TYPE;
# load the users using distributed privileges
# http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
distusers="ndb_dist_priv.sql"
cached_distusers = "#{Chef::Config[:file_cache_path]}/#{distusers}"
Chef::Log.info "Installing #{distusers} to #{cached_distusers}"

cookbook_file "#{cached_distusers}" do
  source "#{distusers}"
  owner "root"
  group "root"
  mode "0755"
  action :create_if_missing
end

