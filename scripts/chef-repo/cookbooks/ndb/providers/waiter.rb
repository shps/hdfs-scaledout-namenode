
action :wait_until_cluster_ready do

  bash "#{new_resource.name}" do
    user "#{node[:ndb][:user]}"
    code <<-EOF
      #{node[:mysql][:base_dir]}/bin/ndb_waiter -c #{node[:ndb][:connect_string]} --timeout=#{node[:ndb][:wait_startup]}  2>&1 > /dev/null
    EOF
  end

end
