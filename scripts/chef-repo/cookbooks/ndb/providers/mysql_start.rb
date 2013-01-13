action :initialize do

# mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql
# SELECT ROUTINE_NAME, ROUTINE_SCHEMA, ROUTINE_TYPE 
# ->     FROM INFORMATION_SCHEMA.ROUTINES 
# ->     WHERE ROUTINE_NAME LIKE 'mysql_cluster%'
# ->     ORDER BY ROUTINE_TYPE;
# load the users using distributed privileges
# http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
distusers="ndb_dist_priv.sql"
cached_distusers = "#{Chef::Config[:file_cache_path]}/#{distusers}"
Chef::Log.info "Installing #{distusers} to #{cached_distusers}"

cookbook_file "#{cached_distusers}" do
  source "#{distusers}"
  owner "root"
  group "root"
  mode "0755"
  action :create_if_missing
end


  bash 'mysql_install_db' do
    code <<-EOF
    cd #{node[:mysql][:base_dir]}
    # --force causes mysql_install_db to run even if DNS does not work. In that case, grant table entries that normally use host names will use IP addresses.
    #{node[:mysql][:base_dir]}/scripts/mysql_install_db --basedir=#{node[:mysql][:base_dir]} --defaults-file=#{node[:ndb][:base_dir]}/my.cnf --force 
    EOF
    not_if { ::File.exists?( "#{node[:ndb][:mysql_server_dir]}/mysql" ) }
  end

  bash "#{new_resource.name}" do
    code <<-EOF
    #{node[:ndb][:scripts_dir]}/mysql-server-start.sh
    EOF
  end

  bash 'create_distributed_privileges' do
    code <<-EOF
     #{node[:ndb][:scripts_dir]}/mysql-client.sh < #{cached_distusers}
     #{node[:ndb][:scripts_dir]}/mysql-client.sh -e "CALL mysql.mysql_cluster_move_privileges" mysql
     echo "Verifying successful conversion of tables.."
     #{node[:ndb][:scripts_dir]}/mysql-client.sh -e "SELECT CONCAT('Conversion ', IF(mysql.mysql_cluster_privileges_are_distributed(), 'succeeded', 'failed'), '.') AS Result;" mysql | grep "Conversion succeeded" 
    EOF
   not_if { `#{node[:ndb][:scripts_dir]}/mysql-client.sh -e "SELECT CONCAT('Conversion ', IF(mysql.mysql_cluster_privileges_are_distributed(), 'succeeded', 'failed'), '.') AS Result;" mysql | grep "Conversion succeeded"` }
  end


  # TODO - UPDATE mysql.user SET Password = PASSWORD('#{node[:mysql][:password]}') User = 'root';
  # FLUSH PRIVILEGES;
  # However, then we also need to modify 'mysql-client.sh' as well.
  bash "grant_users_mysql" do
    code <<-EOF
   #{node[:ndb][:scripts_dir]}/mysql-client.sh -e "GRANT ALL PRIVILEGES on *.* TO '#{node[:mysql][:user]}'@'%' IDENTIFIED BY '#{node[:mysql][:password]}';"
    EOF
    not_if {`#{node[:ndb][:scripts_dir]}/mysql-client.sh -e "SELECT User FROM mysql.user;" mysql | grep #{node[:mysql][:user]}` }
  end


  memcached_sql="ndb_memcache_metadata.sql"
  memcached = "#{Chef::Config[:file_cache_path]}/#{memcached_sql}"

  cookbook_file "#{memcached}" do
    source "#{memcached_sql}"
    owner "root"
    group "root"
    mode "0755"
    action :create_if_missing
  end

#  http://dev.mysql.com/doc/ndbapi/en/ndbmemcache-overview.html
  bash 'install_memcached_tables' do
    code <<-EOF
     #{node[:ndb][:scripts_dir]}/mysql-client.sh < #{memcached}
    EOF
    not_if { }
  end


end
