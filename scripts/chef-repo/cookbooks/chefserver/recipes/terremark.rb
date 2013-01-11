bash "add_knife_terremark" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
sudo gem install knife-terremark --no-rdoc --no-ri --verbose

echo"

# Terremark
knife[:terremark_password] = "#{node[:terremark_password]}"
knife[:terremark_username] = "#{node[:terremark_username]}"
knife[:terremark_service]  = "#{node[:terremark_service]}"

" >> #{HomeDir}/.chef/knife.rb

touch #{HomeDir}/homebase/.knife_terremark
EOF
not_if "test -f #{HomeDir}/homebase/.knife_terremark"
end
 
