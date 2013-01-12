bash "ndbd_start" do
user "#{node[:ndb][:user]}"
code <<-EOF
#{node[:ndb][:scripts_dir]}/ndbd-start.sh
EOF
end
