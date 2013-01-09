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

HomeDir="/var/lib/chef"
user node[:chef][:user] do
  action :create
  shell "/bin/bash"
  supports :manage_home=>true
  home "#{HomeDir}"
end

bash "add_chef_user_sudoers" do
  user "root"
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

bash "install_chef_keys" do
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


bash "install_chef_rabbitmq_install" do
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

RubyBaseDir="#{HomeDir}/.rvm"
RvmBaseDir="/usr/local/rvm"

# bash "install_rvm" do
#   user "#{node[:chef][:user]}"
#   ignore_failure false
#   code <<-EOF

# # install rvm
# # http://beginrescueend.com/rvm/install/

# if [ ! -e #{RvmBaseDir}/scripts/rvm ]
#    then
#      sudo bash -s stable < <(curl -s https://raw.github.com/wayneeseguin/rvm/master/binscripts/rvm-installer)
#      #  #{Chef::Config[:file_cache_path]}/rvm-installer stable
#      fi

#      sudo usermod -a -G rvm #{node[:chef][:user]}
#      source /etc/profile.d/rvm.sh
#      umask u=rwx,g=rwx,o=rx

#      echo "export PATH=$PATH:#{node[:ruby][:base_dir]}/bin" >> #{HomeDir}/.bash_aliases

#      EOF
#   not_if "test -f #{HomeDir}/.bash_aliases || `grep ruby #{HomeDir}/.bash_aliases`"
# end

# bash "install_chef_ruby" do
# user "#{node[:chef][:user]}"
# ignore_failure false
# code <<-EOF
# sudo su -l #{node[:chef][:user]} -c "rvm user all; rvm install 1.9.3; rvm use 1.9.3 --default"
# #sudo su - #{node[:chef][:user]} -l -c "rvm install 1.9.2; rvm use 1.9.2 --default"

# # install these ruby libs (if we don't already have them)
#  . #{RvmBaseDir}/scripts/rvm
#  [ -e #{RubyBaseDir}/usr/lib/libz.so ] || sudo su -l #{node[:chef][:user]} -c "rvm pkg install zlib --verify-downloads 1"
#  [ -e #{RubyBaseDir}/usr/lib/libssl.so ] || sudo su -l #{node[:chef][:user]} -c "rvm pkg install openssl"
#  [ -e #{RubyBaseDir}/usr/lib/libyaml.so ] || sudo su -l #{node[:chef][:user]} -c "rvm pkg install libyaml"


# # check if have the right version of ruby with the correct libs available,
# # if not we reinstall

# # ! (#{RvmBaseDir}/bin/rvm use 1.9.3 && #{RubyBaseDir}/bin/ruby -e "require 'openssl' ; require 'zlib'" 2> /dev/null) && sudo #{RvmBaseDir}/bin/rvm reinstall 1.9.3 && #{RvmBaseDir}/bin/rvm use 1.9.3 --default

# EOF
#   not_if "#{RubyBaseDir}/bin/ruby -v | grep \"1.9.3\" && test -f #{RubyBaseDir}/usr/lib/libssl.so"
# end


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
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
#sudo true && curl -L https://www.opscode.com/chef/install.sh | sudo bash

# Following doesn't work
# sudo #{Chef::Config[:file_cache_path]}/install-chef-solo.sh

echo "chef ALL = (root) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/chef
sudo chmod 0440 /etc/sudoers.d/chef
sudo usermod -s /bin/bash #{node[:chef][:user]}
#sudo chef-solo -v
EOF
not_if "test -f /etc/sudoers.d/chef"
end

bash "install_chef_solo" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
sudo apt-get install chef

sudo apt-get install ruby1.9.1 ruby1.9.1-dev rubygems1.9.1 irb1.9.1 ri1.9.1 rdoc1.9.1 build-essential libopenssl-ruby1.9.1 libssl-dev zlib1g-dev

sudo update-alternatives --install /usr/bin/ruby ruby /usr/bin/ruby1.9.1 400 --slave   /usr/share/man/man1/ruby.1.gz ruby.1.gz \
                        /usr/share/man/man1/ruby1.9.1.1.gz \
        --slave   /usr/bin/ri ri /usr/bin/ri1.9.1 \
        --slave   /usr/bin/irb irb /usr/bin/irb1.9.1 \
        --slave   /usr/bin/rdoc rdoc /usr/bin/rdoc1.9.1

# choose your interpreter
# changes symlinks for /usr/bin/ruby , /usr/bin/gem
# /usr/bin/irb, /usr/bin/ri and man (1) ruby
sudo update-alternatives --config ruby
sudo update-alternatives --config gem

# now try
ruby --version
EOF
end

bash "install_chef_server_from_solo" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
chef-solo -c /etc/chef/solo.rb -j /etc/chef/chef.json -r http://s3.amazonaws.com/chef-solo/bootstrap-latest.tar.gz
#sudo #{node[:ruby][:base_dir]}/bin/chef-solo -c /etc/chef/solo.rb -j /etc/chef/chef.json -r http://s3.amazonaws.com/chef-solo/bootstrap-latest.tar.gz
EOF
not_if "which chef-server"
end


template "#{Chef::Config[:file_cache_path]}/knife-config.sh" do
  source "knife-config.sh.erb"
  owner "chef"
  group "chef"
  mode 0755
end

bash "configure_knife" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
# #{Chef::Config[:file_cache_path]}/knife-config.sh
EOF
not_if "test -f #{HomeDir}/knife.rb"
end



for install_gem in node[:chef][:gems]
  cookbook_file "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem" do
    source "#{install_gem}.gem"
    owner node[:chef][:user]
    group node[:chef][:user]
    mode 0755
    action :create_if_missing
  end
   # gem_package "#{install_gem}" do
   #   source "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem"
   #   action :install
   # end
end

AllGems=node[:chef][:gems].join(" ")

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
# # source /etc/profile.d/rvm.sh
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
# # #source /etc/profile.d/rvm.sh

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
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
CHEF_HOME=#{HomeDir}
CHEF_HOMEBASE=$CHEF_HOME/homebase
echo "export CHEF_USERNAME=#{node[:chef][:user]}" >> /etc/profile 
echo "export CHEF_HOMEBASE=#{HomeDir}/homebase" >> /etc/profile 
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
cp -a $CHEF_HOMEBASE/knife/example-credentials $CHEF_HOMEBASE/knife/credentials
cp /etc/chef/webui.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:client]}.pem
cp /etc/chef/validation.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:org]}-validator.pem
sudo chown -R #{node[:chef][:org]} $CHEF_HOMEBASE/knife/credentials/
cd $CHEF_HOMEBASE/knife/credentials/
mkdir certificates
mkdir client_keys
mkdir data_bag_keys
mkdir ec2_certs
mkdir ec2_keys

# TODO: Copy my cookbooks and roles to homebase

mkdir -p $CHEF_HOMEBASE/tmp/.ironfan-clusters
 # cd $CHEF_HOMEBASE
 # knife cookbook upload -a
 # for role in $CHEF_HOMEBASE/cookbooks/roles/*.rb
 # do 
 #   knife role from file $role 
 # done
touch $CHEF_HOMEBASE/tmp/.installed
EOF
not_if "test -f #{HomeDir}/homebase/tmp/.installed"
end

template "#{HomeDir}/homebase/knife/credentials/knife-org.rb" do
  source "knife-org.rb.erb"
  owner node[:chef][:user]
  group node[:chef][:user]
  mode 0755
end

template "#{HomeDir}/homebase/knife/credentials/knife-user-#{node[:chef][:user]}.rb" do
  source "knife-user.rb.erb"
  owner node[:chef][:user]
  group node[:chef][:user]
  mode 0755
end

template "#{HomeDir}/homebase/clusters/test_cluster.rb" do
  source "test_cluster.rb.erb"
  owner node[:chef][:user]
  group node[:chef][:user]
  mode 0755
end

# knife cluster list
# knife cluster show test_cluster
# knife cluster launch test_cluster
# knife cluster sync test_cluster
# knife cluster bootstrap test_cluster
# knife cluster bootstrap test_cluster-web-0 
# knife cluster bootstrap test_cluster-database-0 

bash "upload_roles_cookbooks" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
cd #{HomeDir}/homebase
rake upload_cookbooks
rake roles

if [ `knife environment show dev` != "0" ]
then
  knife environment create dev
fi

touch #{HomeDir}/homebase/tmp/.uploads_complete
EOF
not_if "test -f #{HomeDir}/homebase/tmp/.uploads_complete"
end
