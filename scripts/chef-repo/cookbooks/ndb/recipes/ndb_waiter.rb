bash "wait_until_mysql_cluster_has_started" do
  user "#{node[:ndb][:user]}"
ignore_failure false
code <<-EOF
  #{node[:mysql][:base_dir]}/bin/ndb_waiter -c #{[:ndb][:connect_string]} --timeout=#{node[:ndb][:wait_to_start_timeout]} 2>&1 > /dev/null
EOF
end
