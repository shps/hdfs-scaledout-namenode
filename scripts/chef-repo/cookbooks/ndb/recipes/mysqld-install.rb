Chef::Log.info "Installing user and other databases in MySQL"

ndb_mysql_start "install" do
  action :install_distributed_privileges
end

