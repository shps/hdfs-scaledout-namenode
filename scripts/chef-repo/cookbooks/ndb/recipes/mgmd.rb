include_recipe "ndb"

require 'fileutils'
require 'inifile'

directory node[:ndb][:mgm_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  recursive true
end


for script in node[:mgm][:scripts] do
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner "root"
    group "root"
    mode 0655
    variables({
       :ndb_dir => node[:ndb][:base_dir],
       :mysql_dir => node[:mysql][:base_dir],
       :connect_string => node[:ndb][:connect_string],
    })
  end
end 

service "ndb_mgmd" do
  supports :restart => true, :stop => true, :start => true
  action [ :nothing ]
end

template "/etc/init.d/ndb_mgmd" do
  source "ndb_mgmd.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0655
  variables({
              :ndb_dir => node[:ndb][:base_dir],
              :mysql_dir => node[:mysql][:base_dir],
              :connect_string => node[:ndb][:connect_string],
              :node_id => @id
            })
  notifies :enable, resources(:service => "ndb_mgmd")
  notifies :restart, resources(:service => "ndb_mgmd")
end

# if File.exist?(node[:ndb][:kthfs_config]) then
#     ini_file = IniFile.load(node[:ndb][:kthfs_config], :comment => ';#')
#     if ini_file.has_section?('hdfs1-mysqlcluster') then
#       Chef::Log.info "Over-writing an existing mysqlcluster section in the ini file."
#       ini_file.delete_section("hdfs1-mysqlcluster")
#     end
#     if ini_file.has_section?('hdfs1-mgmserver') then
#       Chef::Log.info "Over-writing an existing mgmserver section in the ini file."
#       ini_file.delete_section("hdfs1-mgmserver")
#     end
# else 
#   ini_file = IniFile.new(:filename => #{node[:ndb][:kthfs_config]})
# end

# ini_file['hdfs1-mysqlcluster'] = {
#   'status' => 'Stopped',
#   'instance' => 'hdfs1',
#   'service-group'  => 'mysqlcluster',
#   'stop-script'  => "#{node[:ndb][:scripts_dir]}/cluster-shutdown.sh",
#   'start-script'  => "",
#   'pid-file'  => "",
#   'stdout-file'  => "#{node[:ndb][:log_dir]}/cluster.log",
#   'stderr-file'  => "",
#   'start-time'  => ''
# } 

# ini_file['hdfs1-mgmserver'] = {
#   'status' => '',
#   'instance' => '',
#   'service-group'  => 'mysqlcluster',
#   'stop-script'  => "#{node[:ndb][:scripts_dir]}/mgm-server-stop.sh",
#   'start-script'  => "#{node[:ndb][:scripts_dir]}/mgm-server-start.sh",
#   'pid-file'  => "#{node[:ndb][:log_dir]}/ndb_63.pid",
#   'stdout-file'  => "#{node[:ndb][:log_dir]}/ndb_63.out.log",
#   'stderr-file'  => "#{node[:ndb][:log_dir]}/ndb_63.err.log",
#   'start-time'  => ''
# } 

# ini_file.save
