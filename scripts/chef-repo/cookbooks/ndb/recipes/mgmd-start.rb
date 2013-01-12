bash "mgmd_start" do
user "#{node[:ndb][:user]}"
code <<-EOF
#{node[:ndb][:scripts_dir]}/mgm-server-start.sh
EOF
end
