HomeDir="#{node[:chef][:base_dir]}"

for install_package in %w{ git }
  package "#{install_package}" do
    action :install
  end
end


# for install_gem in node[:ironfan][:gems]
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
# #    options "--no-rdoc --no-ri"
#     options(:ignore_dependencies => true)
#   end
# end

template "/etc/profile.d/ironfan.sh" do
  source "ironfan.sh.erb"
  owner "root"
  group "root"
  mode 0755
end

bash "configure_ironfan" do
user "#{node[:chef][:user]}"
code <<-EOF


 # for gem in `ls #{Chef::Config[:file_cache_path]}/*.gem`
 # do
 #   # # if [ ! "`/usr/bin/gem1.9.1 list | grep \"${gem} \"`" ]
 #   # # then
 #   #   echo "INSTALLING: ${gem}"
 #     sudo su -l #{node[:chef][:user]} -c "/usr/bin/gem1.9.1 install #{Chef::Config[:file_cache_path]}/${gem}.gem --no-rdoc --no-ri --ignore-dependencies" 
 #   # fi
 # done


 sudo gem install ironfan --no-rdoc --no-ri
 sudo gem install bundle --no-rdoc --no-ri
# sudo gem install chozo -v '0.3.0' --no-rdoc --no-ri


export CHEF_HOME=#{HomeDir}
export CHEF_HOMEBASE=$CHEF_HOME/homebase
export EDITOR=vi
#echo "export CHEF_USERNAME=#{node[:chef][:user]}" >> #{HomeDir}/.bash_aliases 
#echo "export CHEF_HOMEBASE=#{HomeDir}/homebase" >> #{HomeDir}/.bash_aliases 
#echo "export EDITOR=vi" >> #{HomeDir}/.bash_aliases 
# PATH_UPDATER=/etc/profile.d/ironfan.sh
# sudo echo "export CHEF_USERNAME=#{node[:chef][:user]}" > sudo $PATH_UPDATER
# sudo echo "export CHEF_HOMEBASE=#{HomeDir}/homebase" >> sudo $PATH_UPDATER
# sudo echo "export EDITOR=vi" >> sudo $PATH_UPDATER
# sudo chmod 755 $PATH_UPDATER

cd $CHEF_HOME

git clone https://github.com/infochimps-labs/ironfan-homebase homebase
cd homebase

EOF
end

# Add these files to homebase:
# knife/
#   credentials/
#      knife-user-{username}.rb
#      {username}.pem
#      {organization}-validator.pem

bash "configure_ironfan_bundle" do
user "#{node[:chef][:user]}"
code <<-EOF
export CHEF_HOME=#{HomeDir}
export CHEF_HOMEBASE=$CHEF_HOME/homebase
export EDITOR=vi

# TODO - 'bundle install' not working !?!
#bundle install
sudo bundle install
git submodule update --init
git submodule foreach git checkout master

test -f $CHEF_HOME/.chef && rm -rf $CHEF_HOME/.chef
ln -sni $CHEF_HOMEBASE/knife $CHEF_HOME/.chef

rm -rf $CHEF_HOMEBASE/knife/credentials
cp -a $CHEF_HOMEBASE/knife/example-credentials $CHEF_HOMEBASE/knife/credentials
#cp #{HomeDir}/*.pem/etc/chef/webui.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:client]}.pem
#cp #{HomeDir}/webuit.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:client]}.pem
#sudo chown -R #{node[:chef][:org]} $CHEF_HOMEBASE/knife/credentials/
cp #{HomeDir}/#{node[:chef][:client]}.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:client]}.pem
cp #{HomeDir}/webuit.pem $CHEF_HOMEBASE/knife/credentials/
cp #{HomeDir}/validation.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:org]}-validator.pem
cd $CHEF_HOMEBASE/knife/credentials/
mkdir certificates
mkdir client_keys
mkdir data_bag_keys
mkdir ec2_certs
mkdir ec2_keys
# copy instead of move to make the recipe idempotent wrt chefserver installation.
#cp #{HomeDir}/#{node[:chef][:user]}.pem $CHEF_HOME/.chef/credentials/#{node[:chef][:user]}.pem

touch $CHEF_HOMEBASE/.installed
EOF
not_if "test -f $CHEF_HOMEBASE/.installed"
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
code <<-EOF
source #{HomeDir}/.bashrc
cd #{HomeDir}/homebase
git clone https://github.com/infochimps-labs/ironfan-pantry.git pantry

# 'rake roles' doesn't work, and knife is recommended for uploading roles
knife role from file roles/*.rb

if (knife role list | wc -l) < 2 ; then
 exit 1
fi

# Copy all of ironfan's recipes to the chef server
knife cookbook upload -a -o ./pantry/cookbooks/
if (knife cookbook list | wc -l) < 2 ; then
 exit 1
fi

#knife environment create dev -y
#knife environment create prod -y
#knife environment create stag -y
touch #{HomeDir}/homebase/.uploads_complete
EOF
not_if "test -f #{HomeDir}/homebase/.uploads_complete"
end



# activemodel (3.2.11)
# activesupport (3.2.11)
# addressable (2.3.2)
# archive-tar-minitar (0.5.2)
# berkshelf (1.1.2, 1.1.1)
# builder (3.1.4, 3.0.4)
# bundle (0.0.1)
# bundler (1.2.3)
# bunny (0.8.0, 0.7.9)
# celluloid (0.12.4)
# chef (10.16.6, 10.16.4)
# chef-server-api (10.16.6)
# chef-server-webui (10.16.6)
# chef-solr (10.16.6)
# childprocess (0.3.6)
# chozo (0.4.2)
# coderay (1.0.8)
# configliere (0.4.18)
# cucumber (1.2.1)
# daemons (1.1.9)
# dep_selector (0.0.8)
# diff-lcs (1.1.3)
# erubis (2.7.0)
# eventmachine (1.0.0)
# excon (0.16.10)
# extlib (0.9.16)
# facter (1.6.17)
# faraday (0.8.4)
# ffi (1.2.0, 1.0.11)
# fog (1.8.0)
# formatador (0.2.4)
# gherkin (2.11.5)
# git (1.2.5)
# gorillib (0.5.0, 0.4.2)
# grit (2.5.0)
# guard (1.6.1)
# guard-chef (0.0.2)
# guard-cucumber (1.3.2, 1.3.0)
# guard-process (1.0.5)
# haml (3.1.7)
# hashie (1.2.0)
# highline (1.6.15)
# i18n (0.6.1)
# ipaddress (0.8.0)
# ironfan (4.7.4, 4.7.1)
# jeweler (1.8.4)
# json (1.7.5, 1.6.1, 1.5.4)
# knife-ec2 (0.6.2)
# listen (0.7.2, 0.7.0)
# log4r (1.1.10)
# lumberjack (1.0.2)
# merb-assets (1.1.3)
# merb-core (1.1.3)
# merb-haml (1.1.3)
# merb-helpers (1.1.3)
# merb-param-protection (1.1.3)
# method_source (0.8.1)
# mime-types (1.19)
# minitar (0.5.4)
# mixlib-authentication (1.3.0)
# mixlib-cli (1.3.0, 1.2.2)
# mixlib-config (1.1.2)
# mixlib-log (1.4.1)
# mixlib-shellout (1.1.0)
# moneta (0.7.1, 0.6.0)
# multi_json (1.5.0)
# multipart-post (1.1.5)
# net-http-persistent (2.8)
# net-scp (1.0.4)
# net-ssh (2.6.2, 2.2.2)
# net-ssh-gateway (1.1.0)
# net-ssh-multi (1.1)
# nokogiri (1.5.6)
# ohai (6.14.0)
# polyglot (0.3.3)
# posix-spawn (0.3.6)
# pry (0.9.10)
# rack (1.4.4)
# rake (10.0.3)
# rdoc (3.12)
# redcarpet (2.2.2)
# rest-client (1.6.7)
# ridley (0.6.3, 0.6.2)
# rspec (2.12.0)
# rspec-core (2.12.2)
# rspec-expectations (2.12.1)
# rspec-mocks (2.12.1)
# ruby-hmac (0.4.0)
# ruby-openid (2.2.2)
# ruby_gntp (0.3.4)
# slop (3.3.3)
# solve (0.4.1)
# spoon (0.0.1)
# systemu (2.5.2)
# thin (1.5.0)
# thor (0.16.0)
# timers (1.0.2)
# treetop (1.4.12)
# uuidtools (2.1.3)
# vagrant (1.0.5)
# yajl-ruby (1.1.0)
# yard (0.8.3)
