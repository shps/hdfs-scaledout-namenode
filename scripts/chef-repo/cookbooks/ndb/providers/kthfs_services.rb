
action :install_ndbd do
  ini_file = IniFile.load(node[:ndb][:kthfs_services], :comment => ';#')
  Chef::Log.info "Loaded services for agent into ini-file."

  if ini_file.has_section?("hdfs1-ndb")
    Chef::Log.info "Over-writing an existing section in the ini file."
    ini_file.delete_section("hdfs1-ndb")
  end
  ini_file["hdfs1-ndb"] = {
    'status' => 'Stopped',
    'instance' => "#{node[:ndb][:instance]}",
    'service-group'  => 'mysqlcluster',
    'service'  => 'ndb',
    'init-script'  => "#{node[:ndb][:scripts_dir]}/ndbd-init.sh",
    'stop-script'  => "#{node[:ndb][:scripts_dir]}/ndbd-stop.sh",
    'start-script'  => "#{node[:ndb][:scripts_dir]}/ndbd-start.sh",
    'pid-file'  => "#{node[:ndb][:log_dir]}/ndb_#{new_resource.node_id}.pid",
    'stdout-file'  => "#{node[:ndb][:log_dir]}/ndb_#{new_resource.node_id}_out.log",
    'stderr-file'  => "#{node[:ndb][:log_dir]}/ndb_#{new_resource.node_id}_err.log"
  } 
  ini_file.save
  Chef::Log.info "Saved an updated copy of services file at the kthfsagent."

# Nothing to update in collectd for NDBD

end

action :install_mgmd do
  ini_file = IniFile.load(node[:ndb][:kthfs_services], :comment => ';#')

  if ini_file.has_section?('hdfs1-mysqlcluster') then
    Chef::Log.info "Over-writing an existing mysqlcluster section in the ini file."
    ini_file.delete_section("hdfs1-mysqlcluster")
  end

  if ini_file.has_section?('hdfs1-mgmserver') then
    Chef::Log.info "Over-writing an existing mgmserver section in the ini file."
    ini_file.delete_section("hdfs1-mgmserver")
  end

  ini_file['hdfs1-mysqlcluster'] = {
    'status' => 'Stopped',
    'instance' => "#{node[:ndb][:instance]}",
    'service-group'  => 'mysqlcluster',
    'service'  => 'mysqlcluster',
    'stop-script'  => "#{node[:ndb][:scripts_dir]}/cluster-shutdown.sh",
    'start-script'  => "",
    'pid-file'  => "",
    'stdout-file'  => "#{node[:ndb][:log_dir]}/cluster.log",
    'stderr-file'  => ""
  } 

  ini_file['hdfs1-mgmserver'] = {
    'status' => '',
    'instance' => "#{node[:ndb][:instance]}",
    'service-group'  => 'mysqlcluster',
    'service'  => 'mgmserver',
    'stop-script'  => "#{node[:ndb][:scripts_dir]}/mgm-server-stop.sh",
    'start-script'  => "#{node[:ndb][:scripts_dir]}/mgm-server-start.sh",
    'pid-file'  => "#{node[:ndb][:log_dir]}/ndb_#{new_resource.node_id}.pid",
    'stdout-file'  => "#{node[:ndb][:log_dir]}/ndb_#{new_resource.node_id}.out.log",
    'stderr-file'  => "#{node[:ndb][:log_dir]}/ndb_#{new_resource.node_id}.err.log"
  } 

  ini_file.save
  Chef::Log.info "Saved an updated copy of services file at the kthfsagent." # 

# Nothing to update in collectd for mgmd


end

action :install_mysqld do

  Chef::Log.info "Loading ini-file: #{node[:ndb][:kthfs_services]}"
  ini_file = IniFile.load(node[:ndb][:kthfs_services], :comment => ';#')

  ini_file["hdfs1-mysqld"] = {
    'status' => 'Stopped',
    'instance' => "#{node[:ndb][:instance]}",
    'service-group'  => 'mysqlcluster',
    'service'  => 'mysqld',
    'stop-script'  => "#{node[:ndb][:scripts_dir]}/mysql-server-stop.sh",
    'start-script'  => "#{node[:ndb][:scripts_dir]}/mysql-server-start.sh",
    'pid-file'  => "#{node[:ndb][:log_dir]}/mysql_#{new_resource.node_id}.pid",
    'stdout-file'  => "#{node[:ndb][:log_dir]}/mysql_#{new_resource.node_id}.out.log",
    'stderr-file'  => "#{node[:ndb][:log_dir]}/mysql_#{new_resource.node_id}.err.log"
  } 
  ini_file.save
  Chef::Log.info "Saved an updated copy of services file at the kthfsagent."
  
  bash "update_collectd_#{new_resource.name}" do
   code <<-EOF
     args = "#\n"
     args << "# plugins for #{new_resource.name}\n" 
     args << "#\n"
     args << "LoadPlugin dbi\n"
     args << "<Plugin dbi>\n"
     args << "  <Query \"free_dm\">\n"
     args << "    Statement \"SELECT node_id, total FROM ndbinfo.memoryusage where memory_type LIKE 'Data memory'\"\n"
     args << "      # Use with MySQL 5.0.0 or later\n"
     args << "      MinVersion 50000\n"
     args << "    <Result>\n"
     args << "      Type \"gauge\"\n"
     args << "      InstancePrefix \"free_data_memory\"\n"
     args << "      InstancesFrom \"node_id\"\n"
     args << "      ValuesFrom \"total\"\n"
     args << "    </Result>\n"
     args << "  </Query>\n"
     args << "  <Query \"free_im\">\n"
     args << "    Statement \"SELECT node_id, total FROM ndbinfo.memoryusage where memory_type LIKE 'Index memory'\"\n"
     args << "      # Use with MySQL 5.0.0 or later\n"
     args << "      MinVersion 50000\n"
     args << "    <Result>\n"
     args << "      Type \"gauge\"\n"
     args << "      InstancePrefix \"free_index_memory\"\n"
     args << "      InstancesFrom \"node_id\"\n"
     args << "      ValuesFrom \"total\"\n"
     args << "    </Result>\n"
     args << "  </Query>\n"
     args << "\n"
     args << "  <Database \"ndbinfo\">\n"
     args << "    Driver \"mysql\"\n"
     args << "    DriverOption \"host\" \"#{node['ipaddress']}\"\n"
     args << "    DriverOption \"username\" \"#{node[:mysql][:user]}\"\n"
     args << "    DriverOption \"password\" \"#{node[:mysql][:password]}\"\n"
     args << "    DriverOption \"dbname\" \"ndbinfo\"\n"
     args << "    SelectDB \"ndbinfo\"\n"
     args << "    Query \"free_dm\"\n"
     args << "    Query \"free_im\"\n"
     args << "  </Database>\n"
     args << "</Plugin>\n"
     args << "\n"
     echo $args >> #{node[:collectd][:conf]}
    EOF
    not_if { `grep #{new_resource.name} #{node[:collectd][:conf]}` }
  end


end
