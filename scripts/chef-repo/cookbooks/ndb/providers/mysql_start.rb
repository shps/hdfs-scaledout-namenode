action :initialize do

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
    #    not_if { `#{node[:ndb][:scripts_dir]}/mysql-client.sh -e "SELECT CONCAT('Conversion ', IF(mysql.mysql_cluster_privileges_are_distributed(), 'succeeded', 'failed'), '.') AS Result;" mysql | grep "Conversion succeeded"` }
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

end
