HomeDir="#{node[:chef][:base_dir]}"

for install_package in %w{ git }
  package "#{install_package}" do
    action :install
  end
end


for install_gem in node[:ironfan][:gems]
  cookbook_file "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem" do
    source "#{install_gem}.gem"
    owner node[:chef][:user]
    group node[:chef][:user]
    mode 0755
    action :create_if_missing
  end
  gem_package "#{install_gem}" do
    source "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem"
    action :install
    options "--no-rdoc --no-ri"
  end
end

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

# TODO - bundle not working. Maybe it's a 1.8 version of ruby?
bundle install
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
# copy instead of move to make the recipe idempotent wrt chefserver installation.
cp #{HomeDir}/#{node[:chef][:user]}.pem $CHEF_HOME/.chef/credentials/#{node[:chef][:user]}.pem


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
ignore_failure false
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
