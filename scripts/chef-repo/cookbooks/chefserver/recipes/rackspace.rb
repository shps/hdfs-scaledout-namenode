bash "add_knife_rackspace" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
sudo gem install net-ssh net-ssh-multi fog highline --no-rdoc --no-ri --verbose

echo"

# Rackspace:
knife[:rackspace_api_key]      = "#{node[:rackspace_api_key]}"
knife[:rackspace_api_username] = "#{node[:rackspace_api_username]}"
 
" >> #{HomeDir}/.chef/knife.rb

touch #{HomeDir}/homebase/.knife_rackspace
EOF
not_if "test -f #{HomeDir}/homebase/.knife_rackspace"
end
 
