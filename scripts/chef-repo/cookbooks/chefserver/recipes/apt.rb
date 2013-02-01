# Apt install tutorial
# http://wiki.opscode.com/display/chef/Installing+Chef+Server+Manually

# Actually used this tutorial
# http://jtimberman.housepub.org/blog/2012/11/17/install-chef-10-server-on-ubuntu-with-ruby-1-dot-9/

# Knife/encrypting passwords/creating users tutorial
# http://www.jasongrimes.org/2012/06/provisioning-a-lamp-stack-with-chef-vagrant-and-ec2-2-of-3/

# How to install chef server...
# https://github.com/pikesley/catering-college

# More detailed version of above script here:
# https://github.com/kaldrenon/install-chef-server


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

for install_package in %w{ ruby1.9.1-dev wget build-essential openssl libreadline6 libreadline6-dev curl git zlib1g zlib1g-dev libssl-dev libyaml-dev libsqlite3-dev libxml2-dev libxslt-dev libc6-dev ncurses-dev ssl-cert make expect }
  package "#{install_package}" do
    action :install
  end
end


bash "install_chef_keys" do
  user "#{node[:chef][:user]}"
  code <<-EOF

# needs setting on vagrant VMs for some reason
PATH=${PATH}:/usr/local/sbin:/usr/sbin:/sbin

# add the opscode repo
echo "deb http://apt.opscode.com/ `lsb_release -cs`-0.10 main" | sudo tee /etc/apt/sources.list.d/opscode.list > /dev/null
# and their key
sudo mkdir -p /etc/apt/trusted.gpg.d
echo "TRYING TO LIST KEYS"
EOF
end

bash "install_opscode_apt_keys" do
  user "#{node[:chef][:user]}"
  code <<-EOF
sudo gpg --keyserver keys.gnupg.net --recv-keys 83EF826A
if [ $? -ne 0 ] ; then
  echo "Re-trying opscode key"
  sudo gpg --fetch-key http://apt.opscode.com/packages@opscode.com.gpg.key
  fi

  sudo gpg --export packages@opscode.com | sudo tee /etc/apt/trusted.gpg.d/opscode-keyring.gpg > /dev/null
  if [ ! -s /etc/apt/trusted.gpg.d/opscode-keyring.gpg ] ; then
    sudo mv /etc/apt/trusted.gpg.d/opscode-keyring.gpg.pkg-new /etc/apt/trusted.gpg.d/opscode-keyring.gpg
    fi

    EOF
    # Test file exists and has a size greater than zero.
    not_if "test -s /etc/apt/trusted.gpg.d/opscode-keyring.gpg"
  end

bash "install_rabbitmq_apt_keys" do
 user "#{node[:chef][:user]}"
  code <<-EOF
echo "RabbitMQ KEYS"
# RabbitMQ repo
echo "deb http://www.rabbitmq.com/debian/ testing main" | \
  sudo tee /etc/apt/sources.list.d/rabbit.list > /dev/null
if [ ! "`sudo apt-key list | grep Rabbit`" ]
then
  cd /tmp
  echo "Getting RabbitMQ KEYS"
  wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
  echo "Installing RabbitMQ KEYS"
  sudo apt-key add rabbitmq-signing-key-public.asc
  fi

  # update apt to tell it about the new opscode and rabbitmq repos
  sudo apt-get -y -q update
  EOF
  not_if "`sudo apt-key list | grep Rabbit`"
end

for install_package in %w{ couchdb libgecode-dev rabbitmq-server opscode-keyring }
  package "#{install_package}" do
    action :install
  end
end


bash "install_chef_rabbitmq_install" do
  user "#{node[:chef][:user]}"
  code <<-EOF

`java -version 2> /dev/null` || sudo apt-get -y -q install openjdk-6-jdk

# configure rabbit (if it's not already done)
[ "`sudo rabbitmqctl list_vhosts | grep chef`" ] \
  || sudo rabbitmqctl add_vhost /chef
[ "`sudo rabbitmqctl list_users | grep chef`" ] \
  || sudo rabbitmqctl add_user chef testing
sudo rabbitmqctl set_permissions -p /chef chef ".*" ".*" ".*"
# we also like the rabbit webui management thing
sudo rabbitmq-plugins enable rabbitmq_management
sudo service rabbitmq-server restart

EOF
  not_if "sudo rabbitmqctl -q status 2> /dev/null"
end


for install_package in %w{ ruby1.9.1-dev rubygems1.9.1 irb1.9.1 ri1.9.1 rdoc1.9.1 libopenssl-ruby1.9.1 libssl-dev zlib1g-dev }
  package "#{install_package}" do
    action :install
  end
end

directory "/var/cache/local/preseeding" do
  owner "root"
  mode 0755
  recursive true
end

execute "preseed chef" do
  command "debconf-set-selections /var/cache/local/preseeding/chef.seed"
  action :nothing
end

template "/var/cache/local/preseeding/chef.seed" do
  source "chef.seed.erb"
  owner "root"
  mode "0600"
  notifies :run, resources(:execute => "preseed chef"), :immediately
end

execute "preseed chef-server" do
  command "debconf-set-selections /var/cache/local/preseeding/chef-server.seed"
  action :nothing
end

template "/var/cache/local/preseeding/chef-server.seed" do
  source "chef-server.seed.erb"
  owner "root"
  mode "0600"
  notifies :run, resources(:execute => "preseed chef-server"), :immediately
end

for install_package in %w{ libpolyglot-ruby libtreetop-ruby }
  package "#{install_package}" do
    action :install
#n    options "--force-yes"
  end
end

# TODO - preseeding is supported now in chef. Problem is how to specify password parameter in files/default
# Package expects chef-server.seed to be located in files/default
# package "chef-server" do
#   response_file "chef-server.seed"
# end

for install_package in %w{ chef chef-server chef-server-api chef-server-webui chef-solr chef-expander }
  package "#{install_package}" do
    action :install
#    options "--force-yes"
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
# Wait for chef servers to start
wait_chef=30
timeout=0
while [ $timeout -lt $wait_chef ] ; do
    sleep 1
    ps -ef | grep chef-server > /dev/null
    $? -eq 0 && break
    echo -n "."
    timeout=`expr $timeout + 1`
done
echo "Chef server started in $timeout seconds"

test -f #{HomeDir}/.chef && rm -rf #{HomeDir}/.chef
cd #{HomeDir}
sudo cp /etc/chef/*.pem #{HomeDir}/
sudo chown #{node[:chef][:user]} #{HomeDir}/*.pem
#{Chef::Config[:file_cache_path]}/knife-config.sh
cp #{HomeDir}/.chef/#{node[:chef][:user]}.pem #{HomeDir}/#{node[:chef][:user]}.pem
# For some reason the chef user's shell becomse /bin/sh - change it to bash
#sudo usermod -s /bin/bash #{node[:chef][:user]}

#rubygems update --system
EOF
not_if "test -f #{HomeDir}/#{node[:chef][:user]}.pem || test -f #{HomeDir}/.chef/credentials/#{node[:chef][:user]}.pem"
end
