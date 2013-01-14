action :initialize do

  bash 'mysql_install_db' do
    user "#{node[:ndb][:user]}"
    code <<-EOF
    cd #{node[:mysql][:base_dir]}
    # --force causes mysql_install_db to run even if DNS does not work. In that case, grant table entries that normally use host names will use IP addresses.
    #{node[:mysql][:base_dir]}/scripts/mysql_install_db --basedir=#{node[:mysql][:base_dir]} --defaults-file=#{node[:ndb][:base_dir]}/my.cnf --force 
    EOF
    not_if { ::File.exists?( "#{node[:ndb][:mysql_server_dir]}/mysql" ) }
  end

end

action :install_distributed_privileges do
  # mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql
  # SELECT ROUTINE_NAME, ROUTINE_SCHEMA, ROUTINE_TYPE 
  # ->     FROM INFORMATION_SCHEMA.ROUTINES 
  # ->     WHERE ROUTINE_NAME LIKE 'mysql_cluster%'
  # ->     ORDER BY ROUTINE_TYPE;
  # load the users using distributed privileges
  # http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
  distusers="ndb_dist_priv.sql"
  cached_distusers = "#{Chef::Config[:file_cache_path]}/#{distusers}"
  #Chef::Log.info "Installing #{distusers} to #{cached_distusers}"

  cookbook_file "#{cached_distusers}" do
    source "#{distusers}"
    owner node[:ndb][:user]
    group node[:ndb][:group]
    mode "0755"
    action :create_if_missing
  end

  bash 'create_distributed_privileges' do
    user "#{node[:ndb][:user]}"
    code <<-EOF
     #{node[:ndb][:scripts_dir]}/mysql-client.sh < #{cached_distusers}

     # Test that it works
     #{node[:ndb][:scripts_dir]}/mysql-client.sh -e "CALL mysql.mysql_cluster_move_privileges();" 
     echo "Verifying successful conversion of tables.."
     #{node[:ndb][:scripts_dir]}/mysql-client.sh -e "SELECT CONCAT('Conversion ', IF(mysql.mysql_cluster_privileges_are_distributed(), 'succeeded', 'failed'), '.') AS Result;" | grep "Conversion succeeded" 
    EOF
#    not_if { `#{node[:ndb][:scripts_dir]}/mysql-client.sh -e "SELECT CONCAT('Conversion ', IF(mysql.mysql_cluster_privileges_are_distributed(), 'succeeded', 'failed'), '.') AS Result;"  | grep "Conversion succeeded"` }
    not_if "`#{node[:ndb][:scripts_dir]}/mysql-client.sh -e \"SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME LIKE 'mysql_cluster%';\"  | grep mysql_cluster`"
  end


  # TODO - UPDATE mysql.user SET Password = PASSWORD('#{node[:mysql][:password]}') User = 'root';
  # FLUSH PRIVILEGES;
  # However, then we also need to modify 'mysql-client.sh' as well.
  bash "grant_users_mysql" do
    user "#{node[:ndb][:user]}"
    code <<-EOF
   #{node[:ndb][:scripts_dir]}/mysql-client.sh -e "GRANT ALL PRIVILEGES on *.* TO '#{node[:mysql][:user]}'@'%' IDENTIFIED BY '#{node[:mysql][:password]}';"
    EOF
   not_if "`#{node[:ndb][:scripts_dir]}/mysql-client.sh -e \"SELECT User FROM mysql.user;\" | grep #{node[:mysql][:user]}`"
  end
end

action :install_memcached do
  memcached_sql="ndb_memcache_metadata.sql"
  memcached = "#{Chef::Config[:file_cache_path]}/#{memcached_sql}"
  cookbook_file "#{memcached}" do
    owner node[:ndb][:user]
    group node[:ndb][:group]
    source "#{memcached_sql}"
    mode "0755"
    action :create_if_missing
  end

  #  http://dev.mysql.com/doc/ndbapi/en/ndbmemcache-overview.html
  bash 'install_memcached_tables' do
    user "#{node[:ndb][:user]}"
    code <<-EOF
     #{node[:ndb][:scripts_dir]}/mysql-client.sh < #{memcached}
    EOF
    not_if "`#{node[:ndb][:scripts_dir]}/mysql-client.sh -e \"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=\"ndbmemcache\";\" | grep ndbmemcache`"
  end
end
