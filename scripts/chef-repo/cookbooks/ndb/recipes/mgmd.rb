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
            })
  notifies :enable, resources(:service => "ndb_mgmd")
  notifies :restart, resources(:service => "ndb_mgmd")
end

ndb_kthfs_services node[:ndb][:kthfs_services] do
 action :install_ndbd
    variables({
       :node_id => id
    })
end
