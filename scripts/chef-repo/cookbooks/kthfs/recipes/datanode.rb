
  ini_file = IniFile.load(node[:ndb][:kthfs_services], :comment => ';#')
  Chef::Log.info "Loaded services for agent into ini-file."

  if ini_file.has_section?("kthfs-ndb")
    Chef::Log.info "Over-writing an existing section in the ini file."
    ini_file.delete_section("kthfs-ndb")
  end
  ini_file["kthfs-ndb"] = {
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
