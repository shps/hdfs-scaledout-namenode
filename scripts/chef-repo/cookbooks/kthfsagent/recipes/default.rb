include_recipe "python"
# First install requests: easy_install requests
# Also install bottle: easy_install bottle

# bash "install_python_ruby_libs" do
#    code <<-EOF
#  easy_install requests
#  easy_install bottle
#  gem install inifile
# EOF
# end


easy_install_package "requests" do
  action :install
end

easy_install_package "bottle" do
  action :install
end

gem_package "inifile" do
  source "../files/default/inifile-2.0.2.gem"
  action :install
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
  owner node[:kthfs][:user]
  group node[:kthfs][:user]
  mode 0755
end

template "#{node[:kthfs][:base_dir]}/config.ini" do
  source "config.ini.erb"
  owner node[:kthfs][:user]
  group node[:kthfs][:user]
  mode 0644
  variables({
              :name => node['ipaddress'],
              :rack => '/default'
            })
#    notifies :restart, resources(:service => "ndbd")
end

template "/etc/init.d/kthfsagent" do
  source "kthfsagent.erb"
  owner node[:kthfs][:user]
  group node[:kthfs][:user]
  mode 0655
end

['start-agent.sh', 'stop-agent.sh', 'restart-agent.sh', 'services'].each do |script|
  Chef::Log.info "Installing #{script}"
  template "#{node[:kthfs][:base_dir]}/#{script}" do
    source "#{script}.erb"
    owner node[:kthfs][:user]
    group node[:kthfs][:user]
    mode 0655
  end
end 
