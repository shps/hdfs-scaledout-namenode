include_recipe "ndb"

# if bootstrapping
# node[:ndb][:ndbd] = discover_all(:ndb, :ndbd).map{|svr| [ svr.node[:ndb][:id], svr.node[:ipaddress] ] }.sort!
# 

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
  end
end 

service "ndb_mgmd" do
  supports :restart => true, :stop => true, :start => true
  action :nothing
end

template "/etc/init.d/ndb_mgmd" do
  source "ndb_mgmd.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0754
  notifies :enable, resources(:service => "ndb_mgmd")
end

template "#{node[:ndb][:root_dir]}/config.ini" do
  source "config.ini.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0644
  variables({
              :cores => node[:cpu][:total]
            })
  notifies :restart, resources(:service => "ndb_mgmd"), :immediately
end


#announce(:ndb, :mgm_server)
