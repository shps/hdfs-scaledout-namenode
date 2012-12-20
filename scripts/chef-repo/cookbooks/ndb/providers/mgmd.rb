action :start do
  bash "start-#{new_resource.name}" do
    code <<-EOF
    node[:ndb][:scripts_dir]/mgm-server-start.sh
  EOF
  end
end

action :stop do
  bash "stop-#{new_resource.name}" do
    code <<-EOF
    node[:ndb][:scripts_dir]/mgm-server-stop.sh
  EOF
  end
end

action :restart do
  bash "restart-#{new_resource.name}" do
    code <<-EOF
    node[:ndb][:scripts_dir]/mgm-server-restart.sh
  EOF
  end
end
