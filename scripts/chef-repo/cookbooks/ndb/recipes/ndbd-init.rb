bash "ndbd_init" do
user "#{node[:ndb][:user]}"
code <<-EOF
#{node[:ndb][:scripts_dir]}/ndbd-init.sh
EOF
end
