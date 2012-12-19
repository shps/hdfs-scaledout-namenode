action :install_ndbd do
  ini_file = IniFile.load(node[:ndb][:kthfs_services], :comment => ';#')
  Chef::Log.info "Loaded services for agent into ini-file."

  if ini_file.has_section?("hdfs1-ndb")
    Chef::Log.info "Over-writing an existing section in the ini file."
    ini_file.delete_section("hdfs1-ndb")
  end
  ini_file["test"] = ['a','b']
  ini_file["hdfs1-ndb"] = {
    'status' => 'Stopped',
    'instance' => 'hdfs1',
    'service-group'  => 'mysqlcluster',
    'init-script'  => "#{node[:ndb][:scripts_dir]}/ndbd-init.sh",
    'stop-script'  => "#{node[:ndb][:scripts_dir]}/ndbd-stop.sh",
    'start-script'  => "#{node[:ndb][:scripts_dir]}/ndbd-start.sh",
    'pid-file'  => "#{node[:ndb][:log_dir]}/ndb_#{found_id}.pid",
    'stdout-file'  => "#{node[:ndb][:log_dir]}/ndb_#{found_id}.out.log",
    'stderr-file'  => "#{node[:ndb][:log_dir]}/ndb_#{found_id}.err.log",
    'start-time'  => ''
  } 
  ini_file.save
  Chef::Log.info "Saved an updated copy of services file at the kthfsagent."
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
    'instance' => 'hdfs1',
    'service-group'  => 'mysqlcluster',
    'stop-script'  => "#{node[:ndb][:scripts_dir]}/cluster-shutdown.sh",
    'start-script'  => "",
    'pid-file'  => "",
    'stdout-file'  => "#{node[:ndb][:log_dir]}/cluster.log",
    'stderr-file'  => "",
    'start-time'  => ''
  } 

  ini_file['hdfs1-mgmserver'] = {
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
  Chef::Log.info "Saved an updated copy of services file at the kthfsagent." # 

end

action :install_mysqld do

  Chef::Log.info "Loading ini-file: #{node[:ndb][:kthfs_services]}"
  ini_file = IniFile.load(node[:ndb][:kthfs_services], :comment => ';#')
  # if ini_file.has_section?('hdfs1-mysqld')
  #   Chef::Log.info "Over-writing an existing section in the ini file."
  #   ini_file.delete_section("hdfs1-mysqld")
  # end

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


# load the users using distributed privileges
# http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
# mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql

# mysql_install_db --config=my.cnf...
bash 'mysq_install_db' do
    code <<-EOF
#{node[:ndb][:scripts_dir]}/mysql-server-stop.sh
cd #{node[:mysql][:base_dir]}
# --force causes mysql_install_db to run even if DNS does not work. In that case, grant table entries that normally use host names will use IP addresses.
#{node[:mysql][:base_dir]}/scripts/mysql_install_db --basedir=#{node[:mysql][:base_dir]} --defaults-file=#{node[:ndb][:base_dir]}/my.cnf --force 
EOF
#  not_if { ::File.exists?( "#{node[:ndb][:mysql_data_dir]}" ) }
end


end
