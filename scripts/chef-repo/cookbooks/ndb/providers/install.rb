
notifying_action :create do

  service "ndb-#{new_resource.domain_name}" do
    provider Chef::Provider::Service::Upstart
    supports :start => true, :restart => true, :stop => true
    action :nothing
  end

  template "#{ndb.install}/config.ini" do
    source "config.ini.erb"
    mode "0644"
    cookbook 'ndb'
    variables(:resource => new_resource, :args => args, :authbind => requires_authbind, :listen_ports => [new_resource.admin_port, new_resource.port])
    notifies :restart, resources(:service => "glassfish-#{new_resource.domain_name}"), :delayed
  end
end
