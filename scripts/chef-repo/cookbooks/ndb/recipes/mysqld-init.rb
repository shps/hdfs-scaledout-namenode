Chef::Log.info "Installing user and other databases in MySQL"

ndb_mysql_start "#{node[:ndb][:scripts_dir]}/bin/mysql-server-start.sh" do
  action :initialize
end


