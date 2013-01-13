Chef::Log.info "Installing user and other databases in MySQL"

ndb_mysql_start "start" do
  action :nothing
end


