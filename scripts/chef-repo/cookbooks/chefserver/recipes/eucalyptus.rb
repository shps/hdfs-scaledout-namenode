bash "add_knife_eucalyptus" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
sudo gem install knife-eucalyptus --no-rdoc --no-ri --verbose

echo"

# Eucalyptus
knife[:euca_access_key_id]     = "#{node[:euca_access_key_id]}"
knife[:euca_secret_access_key] = "#{node[:euca_secret_access_key]}"
knife[:euca_api_endpoint]      = "#{node[:euca_api_endpoint]}"
" >> #{HomeDir}/.chef/knife.rb

touch #{HomeDir}/homebase/.knife_eycalyptus
EOF
not_if "test -f #{HomeDir}/homebase/.knife_eycalyptus"
end
 
