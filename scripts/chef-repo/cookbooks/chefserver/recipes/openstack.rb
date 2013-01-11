HomeDir="#{node[:chef][:base_dir]}"


bash "add_knife_openstack" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
sudo gem install knife-openstack --no-rdoc --no-ri --verbose

echo"

knife[:openstack_username] = "#{node[:openstack_username]}"
knife[:openstack_password] = "#{node[:openstack_password]}"
### Note: If you are not proxying HTTPS to the OpenStack auth port, the scheme should be HTTP
# "http://cloud.mycompany.com:5000/v2.0/tokens"
knife[:openstack_auth_url] = "#{node[:openstack_auth_url]}"  
knife[:openstack_tenant] = "#{node[:openstack_tenant]}"


" >> #{HomeDir}/.chef/knife.rb

echo "See https://github.com/opscode/knife-openstack for details..."

touch #{HomeDir}/homebase/.knife_openstack
EOF
not_if "test -f #{HomeDir}/homebase/.knife_openstack"
end
 
