include_recipe "python"


#execute "some_command" do
#  command "command to run once"
#  notifies :create, "ruby_block[some_command_run_flag]", :immediately
#  not_if { node.attribute?("some_command_complete") }
#end
# 
#ruby_block "some_command_run_flag" do
#  block do
#    node.set['some_command_complete'] = true
#    node.save
#  end
#  action :nothing
#end


# First install requests: easy_install requests
# Also install bottle: easy_install bottle

bash "install_python_ruby_libs" do
    code <<-EOF
  easy_install requests
  easy_install bottle
  gem install inifile
EOF
end


user node[:kthfs][:user] do
  action :create
  system true
  shell "/bin/bash"
end

directory node[:kthfs][:base_dir] do
  owner node[:kthfs][:user]
  group node[:kthfs][:user]
  mode "755"
  action :create
  recursive true
end

cookbook_file "#{node[:kthfs][:base_dir]}/agent.py" do
  source "agent.py"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0644
end

template "#{node[:kthfs][:base_dir]}/config.ini" do
  source "config.ini.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0644
  variables({
              :name => node['ipaddress'],
              :rack => '/default'
            })
#    notifies :restart, resources(:service => "ndbd")
end

template "/etc/init.d/kthfsagent" do
  source "kthfsagent.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0655
end

['start-agent.sh', 'stop-agent.sh', 'restart-agent.sh', 'services'].each do |script|
  Chef::Log.info "Installing #{script}"
  template "#{[:kthfs][:base_dir]}/#{script}" do
    source "#{script}.erb"
    owner node[:ndb][:user]
    group node[:ndb][:user]
    mode 0655
  end
end 
