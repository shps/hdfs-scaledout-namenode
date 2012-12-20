action :init do
  bash "init-#{new_resource.name}" do
    code <<-EOF
    node[:ndb][:scripts_dir]/ndbd-init.sh
  EOF
    not_if `"#{node[:ndb][:scripts_dir]}/ndbd-running.sh"`
  end
end

action :start do
  bash "start-#{new_resource.name}" do
    code <<-EOF
    node[:ndb][:scripts_dir]/ndbd-start.sh
  EOF
  end
end

action :stop do
  bash "stop-#{new_resource.name}" do
    code <<-EOF
    node[:ndb][:scripts_dir]/ndbd-stop.sh
  EOF
  end
end

action :restart do
  bash "restart-#{new_resource.name}" do
    code <<-EOF
    node[:ndb][:scripts_dir]/ndbd-restart.sh
  EOF
  end
end
