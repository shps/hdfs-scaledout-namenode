HomeDir="#{node[:chef][:base_dir]}"

bash "add_knife_ec2" do
user "#{node[:chef][:user]}"
ignore_failure false
code <<-EOF
sudo gem install knife-ec2 --no-rdoc --no-ri --verbose

echo"

# EC2 plugin:
knife[:aws_access_key_id]     = "#{node[:aws_access_key_id]}"
knife[:aws_secret_access_key] = "#{node[:aws_secret_access_key]}"

" >> #{HomeDir}/.chef/knife.rb

touch #{HomeDir}/homebase/.knife_ec2
EOF
not_if "test -f #{HomeDir}/homebase/.knife_ec2"
end
