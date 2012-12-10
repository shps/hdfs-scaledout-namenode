
notifying_action :create do

# create the ndbd service in ubuntu
  service "ndbd-#{new_resource.domain_name}" do
#    provider Chef::Provider::Service::Upstart
    supports :start => true, :restart => true, :stop => true
    action :nothing
  end



# Now start the ndbd ...
  service "ndbd-#{new_resource.domain_name}" do
    provider Chef::Provider::Service::Upstart
    supports :start => true, :restart => true, :stop => true
    action [:start]
  end
end

notifying_action :delete do

#  command << "delete-admin-object"
 # command << "--target" << new_resource.target if new_resource.target
#  command << new_resource.name

  bash "asadmin_delete-admin-object #{new_resource.name}" do
#    only_if "#{asadmin_command('list-admin-objects')} | grep -x -- '#{new_resource.name}'"

  end
end
