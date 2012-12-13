notifying_action :restart do
  bash "restart-#{new_resource.name}" do
    code <<-EOF
    node[kthfs:][:base_dir]/restart-agent.sh
  EOF
  end
end
