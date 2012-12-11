include_recipe "ndb"

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
end



args = "[mgmserver]"
args << "status"
args << "instance = "
args << "service-group = mysqlcluster"
args << "stop-script = "
args << "start-script = "
args << "pid-file =  "
args << "stdout-file =  "
args << "stderr-file =  "
args << "start-time = " 

bash "install_mgmd_agent" do
  code <<-EOF
   echo args >> node[:kthfs][:base_dir]/services
not_if 
EOF
end




args = "[mysqlcluster]"
args << "status"
args << "instance = "
args << "service-group = mysqlcluster"
args << "stop-script = "
args << "start-script = "
args << "pid-file =  "
args << "stdout-file =  "
args << "stderr-file =  "
args << "start-time = " 

bash "install_mysqlcluster_agent" do
  code <<-EOF
   echo args >> node[:kthfs][:base_dir]/services
not_if 
EOF
end


#File.open(File.expand_path(File.join(File.dirname(__FILE__),"config.ini")), "r") do |inp|
File.open(File.expand_path(#{node[:kthfs][:base_dir]},"config.ini")), "w") do |inp|
@cfg = ConfigParser.new(inp)
@cfg.
@cfg.add_section "[mysqlcluster]"
@cfg.sections["mysqlcluster"]["status", Float])
