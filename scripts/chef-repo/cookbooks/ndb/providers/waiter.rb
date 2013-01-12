
action :wait_until_cluster_ready do

  bash "#{new_resource.name}" do
    code <<-EOF
    #{new_resource.name} -c #{node[:ndb][:connect_string]} --timeout=#{node[:ndb][:wait_startup]}  2>&1 > /dev/null
    EOF
  end

end
