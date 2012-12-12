notifying_action :start do
  bash "start-#{new_resource.name}" do
    code <<-EOF
    node[ndb:][:scripts_dir]/ndbd-start.sh
  EOF
  end
end

notifying_action :stop do
  bash "stop-#{new_resource.name}" do
    code <<-EOF
    node[ndb:][:scripts_dir]/ndbd-stop.sh
  EOF
  end
end

notifying_action :restart do
  bash "restart-#{new_resource.name}" do
    code <<-EOF
    node[ndb:][:scripts_dir]/ndbd-restart.sh
  EOF
  end
end
