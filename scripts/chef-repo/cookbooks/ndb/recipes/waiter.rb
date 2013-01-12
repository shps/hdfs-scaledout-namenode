bash "ndb_waiter" do
user "#{node[:ndb][:user]}"
ignore_failure false
code <<-EOF

#{node[:mysql][:base_dir]}/bin/ndb_waiter -c #{node[:ndb][:connect_string]} --timeout=#{node[:ndb][:wait_startup]} 2>&1 > /dev/null

EOF
end
