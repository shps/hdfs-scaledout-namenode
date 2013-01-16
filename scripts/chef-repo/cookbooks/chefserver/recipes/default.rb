# Actually used this tutorial
# http://jtimberman.housepub.org/blog/2012/11/17/install-chef-10-server-on-ubuntu-with-ruby-1-dot-9/

# How to install chef server...
# https://github.com/pikesley/catering-college

# More detailed version of above script here:
# https://github.com/kaldrenon/install-chef-server

# ironfan: 
# http://mharrytemp.blogspot.ie/2012/10/getting-started-with-ironfan.html

# How to install chef-server using chef-solo:
# http://wiki.opscode.com/display/chef/Installing+Chef+Server+using+Chef+Solo
# http://blogs.clogeny.com/hadoop-cluster-automation-using-ironfan/


HomeDir="#{node[:chef][:base_dir]}"
user node[:chef][:user] do
  action :create
  shell "/bin/bash"
  supports :manage_home=>true
  home "#{HomeDir}"
end

bash "add_user_sudoers" do
  user "root"
  code <<-EOF
  echo "#{node[:chef][:user]} ALL = (root) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/#{node[:chef][:user]}
  sudo chmod 0440 /etc/sudoers.d/#{node[:chef][:user]}
  EOF
end

for install_package in %w{ruby1.9.1-full build-essential wget ssl-cert curl make expect}
  package "#{install_package}" do
    action :install
  end
end

directory "/etc/chef/certificates" do
  owner "#{node[:chef][:user]}"
  group "#{node[:chef][:user]}"
  action :create
  recursive true
  mode 0755
end

template "/etc/chef/solo.rb" do
  owner "#{node[:chef][:user]}"
  group "#{node[:chef][:user]}"
  source "solo.rb.erb"
  mode 0755
end
 template "/etc/chef/solr.rb" do
   source "solr.rb.erb"
   owner node[:chef][:user]
   group node[:chef][:user]
   mode 0755
 end
template "/etc/chef/chef.json" do
  source "chef.json.erb"
  owner "#{node[:chef][:user]}"
  group "#{node[:chef][:user]}"
  mode 0755
end

template "/etc/chef/server.rb" do
  source "server.rb.erb"
  owner "#{node[:chef][:user]}"
  group "#{node[:chef][:user]}"
  mode 0755
end

template "/etc/chef/webui.rb" do
  source "webui.rb.erb"
  owner "#{node[:chef][:user]}"
  group "#{node[:chef][:user]}"
  mode 0755
end

bash "install_chef_server" do
  user "#{node[:chef][:user]}"
  code <<-EOF
   REALLY_GEM_UPDATE_SYSTEM=yes sudo -E gem update --system
   sudo gem install chef --no-ri --no-rdoc
   sudo chef-solo -o chef-server::rubygems-install
   sudo gem install chef-server-webui --no-ri --no-rdoc
   sudo gem install chef-server-api --no-ri --no-rdoc
   sudo gem install chef-solr --no-ri --no-rdoc

#TODO -  need workaround to get chef-expander installed due to bug:
# chef-expander doesn't work due to https://tickets.opscode.com/browse/CHEF-3567, https://tickets.opscode.com/browse/CHEF-3495
#   sudo gem install chef-expander --no-ri --no-rdoc

   sudo chown -R #{node[:chef][:user]} /var/log/chef 
   sudo chown -R #{node[:chef][:user]} /etc/chef/
   sudo chown -R #{node[:chef][:user]} /var/cache/chef
   sudo chown -R #{node[:chef][:user]} #{HomeDir}

# TODO - also include chef-expander here:
#for file in chef-server chef-solr chef-server-webui 
#do
#  outfile=`basename ${file}`
#  service=${outfile%.conf}
#  sudo ln -sf /lib/init/ upstart-job /etc/init.d/${service}
#  sudo service ${service} start 2> /dev/null || sudo service ${service} restart
#done
  EOF
not_if "which chef-server-webui"
end

# chef-expander 
for install_service in %w{ chef-server chef-solr chef-server-webui }
  service "#{install_service}" do
    provider Chef::Provider::Service::Upstart
    supports :restart => true, :stop => true, :start => true
    action :nothing
  end

  template "/etc/init/#{install_service}.conf" do
    source "#{install_service}.conf.erb"
    owner "#{node[:chef][:user]}"
    group "#{node[:chef][:user]}"
    mode 0755
    notifies :enable, "service[#{install_service}]"
    notifies :start, "service[#{install_service}]", :immediately
  end
end



template "#{Chef::Config[:file_cache_path]}/knife-config.sh" do
  source "knife-config.sh.erb"
  owner "#{node[:chef][:user]}"
  group "#{node[:chef][:user]}"
  mode 0755
end

bash "configure_knife" do
user "#{node[:chef][:user]}"
code <<-EOF

test -f #{HomeDir}/.chef && rm -rf #{HomeDir}/.chef
cd #{HomeDir}
sudo cp /etc/chef/*.pem #{HomeDir}/
sudo chown #{node[:chef][:user]} #{HomeDir}/*.pem
#{Chef::Config[:file_cache_path]}/knife-config.sh
cp #{HomeDir}/.chef/#{node[:chef][:user]}.pem #{HomeDir}/#{node[:chef][:user]}.pem
sudo chown -R #{node[:chef][:user]} #{HomeDir}/*pem
# For some reason the chef user's shell becomse /bin/sh - change it to bash
sudo usermod -s /bin/bash #{node[:chef][:user]}

EOF
#not_if "test -f #{HomeDir}/#{node[:chef][:user]}.pem || test -f #{HomeDir}/.chef/credentials/#{node[:chef][:user]}.pem"
end
