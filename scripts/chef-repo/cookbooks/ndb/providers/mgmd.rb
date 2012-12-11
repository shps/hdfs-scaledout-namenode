notifying_action :init do
  bash "init-#{new_resource.name}" do
    code <<-EOF
    #{[ndb:][:scripts_dir]}/ndbd-init.sh
  EOF
#    not_if "#{[ndb:][:scripts_dir]}/ndbd-running.sh"
  end
end

notifying_action :start do
  bash "start-#{new_resource.name}" do
    code <<-EOF
    #{[ndb:][:scripts_dir]}/ndbd-start.sh
  EOF
  end
end

notifying_action :stop do
  bash "stop-#{new_resource.name}" do
    code <<-EOF
    #{[ndb:][:scripts_dir]}/ndbd-stop.sh
  EOF
  end
end

notifying_action :restart do
  bash "restart-#{new_resource.name}" do
    code <<-EOF
    #{[ndb:][:scripts_dir]}/ndbd-restart.sh
  EOF
  end
end
