include_recipe "ndb"
require 'fileutils'
require 'inifile'


directory "#{node[:ndb][:base_dir]}/mysql/data" do
  owner node[:ndb][:user]
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
  notifies :restart, resources(:service => "mysqld")
end

template "/etc/init.d/mysqld" do
  source "ndbd.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0755
  variables({
              :ndb_dir => node[:ndb][:base_dir],
              :mysql_dir => node[:mysql][:base_dir],
              :connect_string => node[:ndb][:connect_string],
              :node_id => id
            })
  notifies :enable, resources(:service => "mysqld")
  notifies :restart, resources(:service => "mysqld")
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

# load the users using distributed privileges
# http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
# mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql

# mysql_install_db --config=my.cnf...
bash 'mysq_install_db' do
    code <<-EOF
# --force causes mysql_install_db to run even if DNS does not work. In that case, grant table entries that normally use host names will use IP addresses.
#{node[:mysql][:base_dir]}/scripts/mysql_install_db --config=#{node[:ndb][:base_dir]}/my.cnf --force 
EOF
#  not_if { ::File.exists?( node['glassfish']['base_dir'] ) }
end



ini_file = IniFile.load(node[:ndb][:kthfs_services], :comment => ';#')
 if ini_file.has_section?('hdfs1-mysqld')
   Chef::Log.info "Over-writing an existing section in the ini file."
   ini_file.delete_section("hdfs1-mysqld")
 end

ini_file["hdfs1-mysqld"] = {
  'status' => 'Stopped',
  'instance' => 'hdfs1',
  'service-group'  => 'mysqlcluster',
  'stop-script'  => "#{node[:ndb][:scripts_dir]}/mysql-server-stop.sh",
  'start-script'  => "#{node[:ndb][:scripts_dir]}/mysql-server-start.sh",
  'pid-file'  => "#{node[:ndb][:log_dir]}/.pid",
  'stdout-file'  => "#{node[:ndb][:log_dir]}/ndb_63.out.log",
  'stderr-file'  => "#{node[:ndb][:log_dir]}/ndb_63.err.log",
  'start-time'  => ''
} 
ini_file.save
Chef::Log.info "Saved an updated copy of services file at the kthfsagent."
