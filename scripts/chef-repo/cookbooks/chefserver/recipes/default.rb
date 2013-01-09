# How to install chef server...
# https://github.com/pikesley/catering-college

# More detailed version of above script here:
# https://github.com/kaldrenon/install-chef-server

# ironfan: 
# http://mharrytemp.blogspot.ie/2012/10/getting-started-with-ironfan.html

# How to install chef-server using chef-solo:
# http://wiki.opscode.com/display/chef/Installing+Chef+Server+using+Chef+Solo
# http://blogs.clogeny.com/hadoop-cluster-automation-using-ironfan/

# for install_gem in gems
#   cookbook_file "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem" do
#     source "#{install_gem}.gem"
#     owner node[:chef][:user]
#     group node[:chef][:user]
#     mode 0755
#     action :create_if_missing
#   end
#   gem_package "#{install_gem}" do
#     source "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem"
#     action :install
#   end
# end

user  node[:chef][:user] do
  action :create
  shell "/bin/bash"
  supports :manage_home=>true
  home "/home/#{node[:chef][:user]}"
end

bash "add_chef_user_sudoers" do
#user "root"
code <<-EOF
echo "#{node[:chef][:user]} ALL = (root) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/#{node[:chef][:user]}
sudo chmod 0440 /etc/sudoers.d/#{node[:chef][:user]}
EOF
end

for install_package in %w{readline-common libreadline-dev expect expect-dev bind9utils ncurses-dev openssl wget}
   package "#{install_package}" do
     action :install
   end
end

for install_package in %w{build-essential openssl libreadline6 libreadline6-dev curl git-core zlib1g zlib1g-dev libssl-dev libyaml-dev libsqlite3-dev sqlite3 libxml2-dev libxslt-dev autoconf libc6-dev ncurses-dev automake libtool bison subversion}
   package "#{install_package}" do
     action :install
   end
end
 

# directory "#{node[:chef][:base_dir]}" do
#   owner node[:chef][:user]
#   group node[:chef][:user]
#   mode "755"
#   action :create
#   recursive true
# end

# directory "/var/chef" do
#   owner node[:chef][:user]
#   group node[:chef][:user]
#   mode "755"
#   action :create
#   recursive true
# end

# directory "/var/log/chef" do
#   owner node[:chef][:user]
#   group node[:chef][:user]
#   mode "755"
#   action :create
#   recursive true
# end


# template "/etc/chef/solr.rb" do
#   source "solr.rb.erb"
#   owner node[:chef][:user]
#   group node[:chef][:user]
#   mode 0755
# end

# template "/etc/chef/knife.rb" do
#   source "knife.rb.erb"
#   owner node[:chef][:user]
#   group node[:chef][:user]
#   mode 0755
# end
#

bash "install_chef_server1" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
# mostly following this
# http://wiki.opscode.com/display/chef/Installing+Chef+Server+Manually

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
ignore_failure false
code <<-EOF
sudo gpg --list-keys | grep 83EF826A
#if [ $? -ne 0 ] ; then
#  echo "Couldn't find opscode key"
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
ignore_failure false
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

for install_package in %w{ couchdb nginx libgecode-dev rabbitmq-server opscode-keyring }
  package "#{install_package}" do
    action :install
    options "--force-yes"
  end
end


bash "install_chef_server2a" do
user "#{node[:chef][:user]}"
ignore_failure false
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

RubyBaseDir="/home/#{node[:chef][:user]}/.rvm"
RvmBaseDir="/usr/local/rvm"
GemBaseDir="/opt/vagrant_ruby"

bash "install_rvm" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF

# install rvm
# http://beginrescueend.com/rvm/install/

if [ ! -e #{RvmBaseDir}/scripts/rvm ]
then
  sudo bash -s stable < <(curl -s https://raw.github.com/wayneeseguin/rvm/master/binscripts/rvm-installer)
#  #{Chef::Config[:file_cache_path]}/rvm-installer stable
fi

sudo usermod -a -G rvm #{node[:chef][:user]}
source /etc/profile.d/rvm.sh
umask u=rwx,g=rwx,o=rx

if [ ! -f /home/#{node[:chef][:user]}/.bash_aliases  ] ; then
   echo "umask u=rwx,g=rwx,o=rx" >> /home/#{node[:chef][:user]}/.bash_aliases
   echo "source /etc/profile.d/rvm.sh" >> /home/#{node[:chef][:user]}/.bash_aliases
fi

EOF
not_if "test -f /home/#{node[:chef][:user]}/.bash_aliases || `grep rvm /home/#{node[:chef][:user]}/.bash_aliases`"
end

bash "install_chef_ruby" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF

sudo su -l #{node[:chef][:user]} -c "rvm user all; rvm install 1.9.3; rvm use 1.9.3 --default"
#sudo su - #{node[:chef][:user]} -l -c "rvm install 1.9.2; rvm use 1.9.2 --default"

# install these ruby libs (if we don't already have them)
 . #{RvmBaseDir}/scripts/rvm
 [ -e #{RubyBaseDir}/usr/lib/libz.so ] || sudo su -l #{node[:chef][:user]} -c "rvm pkg install zlib --verify-downloads 1"
 [ -e #{RubyBaseDir}/usr/lib/libssl.so ] || sudo su -l #{node[:chef][:user]} -c "rvm pkg install openssl"
 [ -e #{RubyBaseDir}/usr/lib/libyaml.so ] || sudo su -l #{node[:chef][:user]} -c "rvm pkg install libyaml"


# check if have the right version of ruby with the correct libs available,
# if not we reinstall

# ! (#{RvmBaseDir}/bin/rvm use 1.9.3 && #{RubyBaseDir}/bin/ruby -e "require 'openssl' ; require 'zlib'" 2> /dev/null) && sudo #{RvmBaseDir}/bin/rvm reinstall 1.9.3 && #{RvmBaseDir}/bin/rvm use 1.9.3 --default

EOF
  not_if "#{RubyBaseDir}/bin/ruby -v | grep \"1.9.3\" && test -f #{RubyBaseDir}/usr/lib/libssl.so"
end


directory "/etc/chef" do
  owner "chef"
  group "chef"
  mode "755"
  action :create
  recursive true
end

template "/etc/chef/chef.json" do
  source "chef.json.erb"
  owner "chef"
  group "chef"
  mode 0755
end
 template "/etc/chef/solo.rb" do
  source "solo.rb.erb"
  owner "chef"
  group "chef"
  mode 0755
 end

template "/etc/chef/server.rb" do
  source "server.rb.erb"
  owner "chef"
  group "chef"
  mode 0755
end

template "/etc/chef/webui.rb" do
  source "webui.rb.erb"
  owner "chef"
  group "chef"
  mode 0755
end

for install_file in %w{vhost.template install-chef-solo.sh} 
  cookbook_file "#{Chef::Config[:file_cache_path]}/#{install_file}" do
    source "#{install_file}"
    owner "chef"
    group "chef"
    mode 0755
    action :create_if_missing
  end
end


bash "install_chef_solo" do
user "chef"
ignore_failure false
code <<-EOF
source /etc/profile.d/rvm.sh

#sudo true && curl -L https://www.opscode.com/chef/install.sh | sudo bash
sudo true && #{Chef::Config[:file_cache_path]}/install-chef-solo.sh
chef-solo -v
EOF
not_if "which chef-solo"
end



bash "install_chef_server_from_solo" do
user "chef"
ignore_failure false
code <<-EOF
sudo chef-solo -c /etc/chef/solo.rb -j /etc/chef/chef.json -r http://s3.amazonaws.com/chef-solo/bootstrap-latest.tar.gz
EOF
not_if "chef-server -v"
end


template "#{Chef::Config[:file_cache_path]}/knife-config.sh" do
  source "knife-config.sh.erb"
  owner "chef"
  group "chef"
  mode 0755
end

bash "configure_knife" do
user "chef"
ignore_failure false
code <<-EOF
#{Chef::Config[:file_cache_path]}/knife-config.sh
EOF
end



# for install_gem in node[:chef][:gems]
#   cookbook_file "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem" do
#     source "#{install_gem}.gem"
#     owner node[:chef][:user]
#     group node[:chef][:user]
#     mode 0755
#     action :create_if_missing
#   end
#    # gem_package "#{install_gem}" do
#    #   source "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem"
#    #   action :install
#    # end
# end

# AllGems=node[:chef][:gems].join(" ")

# bash "install_chef_server2e" do
# user "#{node[:chef][:user]}"
# ignore_failure false
# code <<-EOF


# # install the chef gems (if we don't already have them)
# # echo "GEMS: #{AllGems}"
# #  for gem in #{AllGems}
# #  do
# #    if [ ! "`gem list | grep \"${gem} \"`" ]
# #    then
# #      echo "INSTALLING: ${gem}"
# #      sudo su -l #{node[:chef][:user]} -c "gem install #{Chef::Config[:file_cache_path]}/${gem}.gem --no-rdoc " # --force
# #    fi
# #  done
#  for gem in chef-server chef-solr chef-expander chef-server-webui
#  do
#    if [ ! "`gem list | grep \"${gem} \"`" ]
#    then
#      echo "INSTALLING: ${gem}"
#      sudo su -l #{node[:chef][:user]} -c "gem install #{Chef::Config[:file_cache_path]}/${gem}.gem --no-rdoc --no-ri" # 
#    fi
#  done    source "#{upstartService}.erb"
#     owner "root"
#     group "root"
#     mode 0644
#   end
# end

# bash "install_chef_server3" do
# user "#{node[:chef][:user]}"
# ignore_failure false
# code <<-EOF
# source /etc/profile.d/rvm.sh
# sudo chown -R #{node[:chef][:user]} /var/chef/
# # run the solr installer
# sudo su -l #{node[:chef][:user]} -c "chef-solr-installer -f -u #{node[:chef][:user]}"

# for file in chef-server chef-solr chef-expander chef-server-webui
# do
#   outfile=`basename ${file}`
#   service=${outfile%.conf}
#   sudo ln -sf /lib/init/upstart-job /etc/init.d/${service}
#   sudo service ${service} start 2> /dev/null || sudo service ${service} restart
# done
# EOF
# end

# bash "install_chef_server3b" do
# user "#{node[:chef][:user]}"
# ignore_failure false
# code <<-EOF
# #source /etc/profile.d/rvm.sh

# for line in "chef-server:4000:chef chef-webui:4040:chefwebui" 
# do
#   UPSTREAM=`echo ${line} | cut -d ':' -f 1`
#   PORT=`echo ${line} | cut -d ':' -f 2`
#   SERVERNAME=`echo ${line} | cut -d ':' -f 3`.`hostname -f`
#   cat #{Chef::Config[:file_cache_path]}/vhost.template |\
#     sed "s:UPSTREAM:${UPSTREAM}:" |\
#     sed "s:PORT:${PORT}:" |\
#     sed "s:SERVERNAME:${SERVERNAME}:" |\
#     sudo tee /etc/nginx/sites-available/${SERVERNAME} > /dev/null
#   [ ${PORT} == "4040" ] && WEBUI="http://${SERVERNAME}"
#   [ ${PORT} == "4000" ] && CHEFSERVER="http://${SERVERNAME}:4000"
#   sudo ln -sf /etc/nginx/sites-available/${SERVERNAME} /etc/nginx/sites-enabled
# done

# sudo service nginx restart

# echo
# echo "Chef-server is at ${CHEFSERVER}"
# echo "Chef WebUI is at ${WEBUI}"
# echo

# EOF
# end

bash "configure_ironfan" do
user "chef"
ignore_failure false
code <<-EOF
# # echo "source /home/#{node[:chef][:user]}/.ironfan_bashrc" >> /home/#{node[:chef][:user]}/.bash_aliases

#echo "export CHEF_USERNAME=#{node[:chef][:user]}" > /home/#{node[:chef][:user]}/.ironfan_bashrc
#echo "export CHEF_HOMEBASE=/home/#{node[:chef][:user]}/homebase" >> /etc/profile 
echo "export CHEF_HOMEBASE=/var/lib/chef/homebase" >> /etc/profile 
#/home/#{node[:chef][:user]}/.ironfan_bashrc
# CHEF_HOMEBASE=/home/#{node[:chef][:user]}/homebase

CHEF_HOME=/var/lib/chef
CHEF_HOMEBASE=$CHEF_HOME/homebase
cd $CHEF_HOME
git clone https://github.com/infochimps-labs/ironfan-homebase homebase
cd homebase
sudo bundle install
git submodule update --init
git submodule foreach git checkout master

rm -rf $CHEF_HOME/.chef
ln -sni $CHEF_HOMEBASE/knife $CHEF_HOME/.chef

# Add these files to homebase:
# knife/
#   credentials/
#      knife-user-{username}.rb
#      {username}.pem
#      {organization}-validator.pem

rm -rf $CHEF_HOMEBASE/knife/credentials
mv $CHEF_HOMEBASE/knife/example-credentials $CHEF_HOMEBASE/knife/credentials
mv $CHEF_HOMEBASE/knife/credentials/knife-user-example.rb $CHEF_HOMEBASE/knife/credentials/knife-user-#{node[:chef][:user]}.rb
cp /etc/chef/webui.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:client]}.pem
cp /etc/chef/validation.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:org]}-validator.pem

#cp /home/#{node[:chef][:user]}/src/kthfs-ws/distribute/.chef/knife.rb /home/#{node[:chef][:user]}/.chef
#mkdir -p /home/node[:chef][:user]/tmp/.ironfan-clusters
# cd /home/#{node[:chef][:user]}/
# knife cookbook upload -a
# for role in /home/node[:chef][:user]/cookbooks/roles/*.rb
# do 
#   knife role from file $role 
# done
touch $CHEF_HOMEBASE/.installed
EOF
not_if "test -f /var/lib/chef/homebase/.installed"
end

template "/home/#{node[:chef][:user]}/homebase/knife/credentials/knife-user-#{node[:chef][:user]}.rb" do
  source "knife-user.rb.erb"
  owner node[:chef][:user]
  group node[:chef][:user]
  mode 0755
end
