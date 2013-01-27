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
  owner "#{node[:chef][:user]}"
  mode 0755
end

bash "configure_ironfan" do
user "#{node[:chef][:user]}"
ignore_failure false
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
sudo update-alternatives --set ruby /usr/bin/ruby1.9.1
sudo update-alternatives --set gem /usr/bin/gem1.9.1

export CHEF_USERNAME=#{node[:chef][:user]}
export CHEF_HOME=#{HomeDir}
export CHEF_HOMEBASE=$CHEF_HOME/homebase
export EDITOR=vi
cd $CHEF_HOME
git clone https://github.com/infochimps-labs/ironfan-homebase homebase
cd homebase

# Add these files to homebase:
# knife/
#   credentials/
#      knife-user-{username}.rb
#      {username}.pem
#      {organization}-validator.pem

sudo bundle install
git submodule update --init
git submodule foreach git checkout master

test -f $CHEF_HOME/.chef && rm -rf $CHEF_HOME/.chef
ln -sn $CHEF_HOMEBASE/knife $CHEF_HOME/.chef

rm -rf $CHEF_HOMEBASE/knife/credentials
cp -a $CHEF_HOMEBASE/knife/example-credentials $CHEF_HOMEBASE/knife/credentials

# Assume my user's credentials are in my home dir, from a successful run of knife-config.sh
cp #{HomeDir}/#{node[:chef][:user]}.pem #{HomeDir}/homebase/knife/credentials/#{node[:chef][:user]}.pem
# Copy other credentials from chef config dir
sudo cp /etc/chef/webuit.pem $CHEF_HOMEBASE/knife/credentials/
sudo cp /etc/chef/validation.pem $CHEF_HOMEBASE/knife/credentials/#{node[:chef][:org]}-validator.pem
sudo chown #{node[:chef][:user]} $CHEF_HOMEBASE/knife/credentials/*.pem

cd $CHEF_HOMEBASE/knife/credentials/
mkdir certificates
mkdir client_keys
mkdir data_bag_keys
mkdir ec2_certs
mkdir ec2_keys

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
cwd #{HomeDir}/homebase
code <<-EOF
env > /tmp/the_env
export CHEF_USERNAME=#{node[:chef][:user]}
export CHEF_HOME=#{HomeDir}
export CHEF_HOMEBASE =$CHEF_HOME/homebase
export EDITOR=vi

# The bash resource logs in as 'root' but just changes UID to run the process as.
# This isn't the same as logging in as a user, running bash_profile, etc
sudo su - #{node[:chef][:user]} -c "knife role from file #{HomeDir}/homebase/roles/*.rb"
# 'rake roles' doesn't work, and knife is recommended for uploading roles
#if (knife role list | wc -l) < 3 ; then
#
#fi

if [ ! -d pantry ] ; then
  git clone https://github.com/infochimps-labs/ironfan-pantry.git pantry
fi

# Copy all of ironfan's recipes to the chef server

#if (knife cookbook list | wc -l) < 4 ; then
  sudo su - #{node[:chef][:user]} -c "knife cookbook upload -a -o #{HomeDir}/homebase/pantry/cookbooks"
#fi

echo "Create your environments: "
echo "knife environment create dev -y"
echo "knife environment create prod -y"
echo "knife environment create stag -y"

touch #{HomeDir}/homebase/.uploads_complete
EOF
not_if "test -f #{HomeDir}/homebase/.uploads_complete"
end
