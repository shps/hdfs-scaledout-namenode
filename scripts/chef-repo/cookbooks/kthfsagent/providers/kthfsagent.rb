action :restart do
  Chef::Log.info "Restarting kthfsagent..."
  bash "restart-#{new_resource.name}" do
    code <<-EOF
    node[kthfs:][:base_dir]/restart-agent.sh
  EOF
  end
end
