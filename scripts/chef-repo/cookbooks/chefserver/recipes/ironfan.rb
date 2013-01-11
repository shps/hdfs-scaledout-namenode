HomeDir=#{node[:chef][:base_dir]}

bash "configure_ironfan" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF

sudo gem install ironfan --no-rdoc --no-ri
sudo gem install bundle --no-rdoc --no-ri

CHEF_HOME=#{HomeDir}
CHEF_HOMEBASE=$CHEF_HOME/homebase
echo "export CHEF_USERNAME=#{node[:chef][:user]}" >> #{HomeDir}/.bash_aliases 
echo "export CHEF_HOMEBASE=#{HomeDir}/homebase" >> #{HomeDir}/.bash_aliases 
cd $CHEF_HOME

git clone https://github.com/infochimps-labs/ironfan-homebase homebase
cd homebase
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
mv #{HomeDir}/#{node[:chef][:user]}.pem $CHEF_HOME/.chef/credentials/#{node[:chef][:user]}.pem

touch $CHEF_HOMEBASE/tmp/.installed
EOF
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
source #{HomeDir}/.bashrc

knife environment create dev -y
# This doesn't seem to do anything.
rake upload_cookbooks
# Uploading roles to chef server seems to work ok.
rake roles

# Copy all of ironfan's recipes to the chef server
git clone https://github.com/infochimps-labs/ironfan-pantry.git pantry
cd pantry
knife cookbook upload -a -o cookbooks/

touch #{HomeDir}/homebase/tmp/.uploads_complete
EOF
end
