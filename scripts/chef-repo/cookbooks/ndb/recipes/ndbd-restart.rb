bash "ndbd_restart" do
user "#{node[:ndb][:user]}"
code <<-EOF
#{node[:ndb][:scripts_dir]}/ndbd-restart.sh
EOF
end
