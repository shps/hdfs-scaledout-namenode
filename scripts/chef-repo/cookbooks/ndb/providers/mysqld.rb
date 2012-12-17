notifying_action :start do
  bash "start-#{new_resource.name}" do
    code <<-EOF
    node[ndb:][:scripts_dir]/mysql-server-start.sh
  EOF
  end
end

notifying_action :stop do
  bash "stop-#{new_resource.name}" do
    code <<-EOF
    node[ndb:][:scripts_dir]/mysql-server-stop.sh
  EOF
  end
end

notifying_action :restart do
  bash "restart-#{new_resource.name}" do
    code <<-EOF
    node[ndb:][:scripts_dir]/mysql-server-restart.sh
  EOF
  end
end
