HomeDir="#{node[:chef][:base_dir]}"


bash "add_knife_rackspace" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
sudo gem install knife-rackspace --no-rdoc --no-ri --verbose

echo"

# Rackspace:
knife[:rackspace_api_key]      = "#{node[:rackspace_api_key]}"
knife[:rackspace_api_username] = "#{node[:rackspace_api_username]}"
knife[:rackspace_version] = 'v2'
knife[:rackspace_api_auth_url] = "lon.auth.api.rackspacecloud.com"

#DFW_ENDPOINT = 'https://dfw.servers.api.rackspacecloud.com/v2'
#ORD_ENDPOINT = 'https://ord.servers.api.rackspacecloud.com/v2'
#LON_ENDPOINT = 'https://lon.servers.api.rackspacecloud.com/v2'
knife[:rackspace_endpoint] = "https://lon.servers.api.rackspacecloud.com/v2"

" >> #{HomeDir}/.chef/knife.rb

echo "See https://github.com/opscode/knife-rackspace for details..."

touch #{HomeDir}/homebase/.knife_rackspace
EOF
not_if "test -f #{HomeDir}/homebase/.knife_rackspace"
end
 
