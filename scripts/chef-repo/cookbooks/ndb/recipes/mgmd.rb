include_recipe "ndb"
require 'fileutils'
require 'inifile'

directory node[:ndb][:mgm_dir] do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  recursive true
end


for script in node[:mgm][:scripts]
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
  provider Chef::Provider::Service::Init
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
  notifies :restart, resources(:service => "ndb_mgmd")
end


# content = File.read("#{node[:kthfs][:base_dir]}/config.ini")
# ini_file = IniFile.new(content, :comment => ';')

#ini_file = IniFile.load("#{node[:kthfs][:base_dir]}/config.ini", :comment => ';#')
ini_file = IniFile.load("/var/lib/kthfsagent/config.ini", :comment => ';#')

if ini_file.has_section?('hdfs1-mysqlcluster')
  Chef::Log.warn "mysqlcluster already exists in the ini file"
end
ini_file["hdfs1-mysqlcluster"] = {
  'status' => 'Stopped',
  'instance' => 'hdfs1',
  'service-group'  => 'mysqlcluster',
  'stop-script'  => "#{node[:ndb][:scripts_dir]}/cluster-shutdown.sh",
  'start-script'  => "",
  'pid-file'  => "",
  'stdout-file'  => "#{node[:ndb][:log_dir]}/cluster.log",
  'stderr-file'  => "",
  'start-time'  => ''
} 

if ini_file.has_section?('hdfs1-mgmserver')
  Chef::Log.warn "mgmd already exists in the ini file"
end
ini_file["hdfs1-mgmserver"] = {
  'status' => '',
  'instance' => '',
  'service-group'  => 'mysqlcluster',
  'stop-script'  => "#{node[:ndb][:scripts_dir]}/mgm-server-stop.sh",
  'start-script'  => "#{node[:ndb][:scripts_dir]}/mgm-server-start.sh",
  'pid-file'  => "#{node[:ndb][:log_dir]}/ndb_63.pid",
  'stdout-file'  => "#{node[:ndb][:log_dir]}/ndb_63.out.log",
  'stderr-file'  => "#{node[:ndb][:log_dir]}/ndb_63.err.log",
  'start-time'  => ''
} 

ini_file.save


#ini_file.delete_section('namenode')
