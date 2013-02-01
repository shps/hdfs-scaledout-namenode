# ironfan tutorials: 
# http://mharrytemp.blogspot.ie/2012/10/getting-started-with-ironfan.html
# http://blogs.clogeny.com/hadoop-cluster-automation-using-ironfan/

HomeDir="#{node[:chef][:base_dir]}"

for install_package in %w{ git-core libxml2-dev libxslt1-dev }
  package "#{install_package}" do
    action :install
  end
end

# We can reduce installation time by caching all the gems in the recipe, and then
# installing them 'locally'. 
#
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
#     options(:ignore_dependencies => true, :no_rdoc => true, :no_ri => true)
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
cwd "#{HomeDir}"
code <<-EOF

sudo gem install ironfan --no-rdoc --no-ri
sudo gem install bundle --no-rdoc --no-ri
sudo update-alternatives --set ruby /usr/bin/ruby1.9.1
sudo update-alternatives --set gem /usr/bin/gem1.9.1

export CHEF_USERNAME=#{node[:chef][:user]}
export CHEF_HOMEBASE=#{HomeDir}/homebase
export EDITOR=vi
git clone https://github.com/infochimps-labs/ironfan-homebase homebase
cd homebase
sudo bundle install
git submodule update --init
git submodule foreach git checkout master

# Add these files to homebase:
# knife/
#   credentials/
#      knife-user-{username}.rb
#      {username}.pem
#      {organization}-validator.pem

# Ironfan removes the default .chef directory, and replaces it with a symbolic link
# to homebase/knife.
test -f #{HomeDir}/.chef && rm -rf #{HomeDir}/.chef
sudo su - #{node[:chef][:user]} -c "ln -s $CHEF_HOMEBASE/knife #{HomeDir}/.chef"
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

bash "upload_roles" do
user "#{node[:chef][:user]}"
ignore_failure false
cwd "#{HomeDir}/homebase"
code <<-EOF
#env > /tmp/the_env
#export CHEF_USERNAME=#{node[:chef][:user]}
#export CHEF_HOMEBASE =#{HomeDir}/homebase
#export EDITOR=vi

# The bash resource logs in as 'root' but just changes UID to run the process as.
# This isn't the same as logging in as a user, running bash_profile, etc
sudo su - #{node[:chef][:user]} -c "knife role from file #{HomeDir}/homebase/roles/*.rb"

touch #{HomeDir}/homebase/.roles_uploaded
EOF
not_if "test -f #{HomeDir}/homebase/.roles_uploaded"
end

bash "upload_cookbooks" do
user "#{node[:chef][:user]}"
ignore_failure false
cwd "#{HomeDir}/homebase"
code <<-EOF
#export CHEF_USERNAME=#{node[:chef][:user]}
#export CHEF_HOMEBASE =#{HomeDir}/homebase
#export EDITOR=vi

cd #{HomeDir}/homebase
git clone https://github.com/infochimps-labs/ironfan-pantry.git pantry
# Check to make sure git clone worked.
test -d #{HomeDir}/homebase/pantry
# Copy all of ironfan's recipes to the chef server
sudo su - #{node[:chef][:user]} -c "knife cookbook upload -a -o #{HomeDir}/homebase/pantry/cookbooks"

echo "Create your environments: "
echo "knife environment create dev -y"
echo "knife environment create prod -y"
echo "knife environment create stag -y"

touch #{HomeDir}/homebase/.cookbooks_uploaded
EOF
not_if "test -f #{HomeDir}/homebase/.cookbooks_uploaded"
end
