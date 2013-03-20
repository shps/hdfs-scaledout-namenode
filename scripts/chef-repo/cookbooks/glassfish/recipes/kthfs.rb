#
# Copyright Peter Donald
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#include_recipe "ark"
include_recipe "glassfish::default"

domain_name="domain1"
admin_port=4848
port=8080
secure=true
username="admin"
password="admin"
password_file="#{node['glassfish']['domains_dir']}/#{domain_name}_admin_passwd"

glassfish_domain "#{domain_name}" do
  username username
  password password
  password_file username ? "#{node['glassfish']['domains_dir']}/#{domain_name}_admin_passwd" : nil
  port port 
  admin_port admin_port
  secure secure 
  echo true
  terse false
  min_memory 3500
  max_memory 4500
  max_stack_size 1024
  max_perm_size 1024
  action :create
end
#  remote_access false
 # extra_libraries ['http://lucan.sics.se/kthfs/mysql-connector-java-5.1.22-bin.jar']

glassfish_library "http://lucan.sics.se/kthfs/mysql-connector-java-5.1.22-bin.jar" do
  domain_name domain_name
  admin_port admin_port 
  username username 
  password_file password_file 
  secure false
  action :add
end

glassfish_secure_admin domain_name do
   domain_name domain_name
   username username
   password_file password_file 
   admin_port admin_port
   secure false
   echo true
   terse false
   action :enable
end

# bash "asadmin_enable-secure-admin" do
#   user node['glassfish']['user']
#   group node['glassfish']['group']
#   ignore_failure true
#   code <<-EOF
# # netstat -npl | grep 8686
#   echo "Bash enabling secure admin "
#    #{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd enable-secure-admin
#   EOF
#   not_if "{node['glassfish']['base_dir']}/glassfish/bin/asadmin get secure-admin.enabled | grep -x -- 'secure-admin.enabled=true'"
# end

# #TODO - this should only get executed on the first install
 #  bash "asadmin_restart-server" do
 #   user node['glassfish']['user']
 #   group node['glassfish']['group']
 #   ignore_failure true
 #   code <<-EOF
 #   echo "Bash restarting domain "
 #   #{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd restart-domain
 #   EOF
 # end




  # asadmin --user admin --passwordfile gfpass create-jdbc-connection-pool
  #    --datasourceclassname com.derby.jdbc.jdbcDataSource
  #    --property user=dbuser:passwordfile=dbpasswordfile:
  #    DatabaseName=jdbc\\:derby:server=http\\://localhost\\:9092 javadb-pool



#glassfish_auth_realm "Authentication Real" do
#   action :enable
#end
kthfs_db = "kthfs"

#  glassfish_jdbc_connection_pool kthfs_db do
#    domain_name domain_name
#    admin_port admin_port 
#    username username 
#    password_file password_file 
#    secure false
#    restype "javax.sql.DataSource"
#    datasourceclassname "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
#    validationmethod "auto-commit"
#    isolationlevel "read-committed"
#    maxpoolsize 32
#    steadypoolsize 16
#    poolresize 2
#    idletimeout 300
# #   properties { "user=root:password=kthfs:databaseName=kthfs:port=3306:serverName=localhost:url=\\\"jdbc\\:mysql\\://localhost\\:3306/kthfs\\\""}
#    properties { "user=root:password=kthfs:databaseName=kthfs:port=3306:serverName=localhost"}
#  end

 bash "install_jdbc" do
   user node['glassfish']['user']
   group node['glassfish']['group']
   code <<-EOF
   #{node['glassfish']['base_dir']}/glassfish/bin/asadmin create-jdbc-connection-pool -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd --datasourceclassname com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource --restype javax.sql.DataSource --nontransactionalconnections=true --creationretryattempts=1 --creationretryinterval=2 --validationmethod=auto-commit --isconnectvalidatereq=true --isolationlevel=read-committed --property user=root:password=kthfs:databaseName=kthfs:portNumber=3307:serverName=localhost:url="jdbc\\:mysql\\://localhost\\:3307/kthfs" #{kthfs_db}
   #{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd create-jdbc-resource --connectionpoolid #{kthfs_db} --enabled=true jdbc/#{kthfs_db}
   EOF
   not_if "#{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd list-jdbc-connection-pools | grep -i #{kthfs_db}"
 end


 # glassfish_jdbc_resource "jdbc/#{kthfs_db}" do
 #   domain_name domain_name
 #   admin_port admin_port 
 #   username username 
 #   password_file password_file 
 #   secure false
 #   connectionpoolid "#{kthfs_db}"
 # end

# <auth-realm name="DBRealm" classname="com.sun.enterprise.security.auth.realm.jdbc.JDBCRealm">
#   <property name="jaas-context" value="jdbcRealm"></property>
#   <property name="password-column" value="PASSWORD"></property>
#   <property name="assign-groups" value="ADMIN,USER"></property>
#   <property name="datasource-jndi" value="jdbc/kthfs"></property>
#   <property name="group-table" value="USERS_GROUPS"></property>
#   <property name="user-table" value="USERS"></property>
#   <property name="group-name-column" value="groupname"></property>
#   <property name="digestrealm-password-enc-algorithm" value="none"></property>
#   <property name="group-table-user-name-column" value="email"></property>
#   <property name="digest-algorithm" value="none"></property>
#   <property name="user-name-column" value="EMAIL"></property>
#   <property name="encoding" value="Hex"></property>
#   <property name="db-user" value="root"></property>
#   <property name="db-password" value="kthfs"></property>
# </auth-realm>

 # glassfish_auth_realm "DBRealm" do
 #   jaas_context "jdbcRealm"
 #   classname "com.sun.enterprise.security.auth.realm.jdbc.JDBCRealm"
 #   assign_groups { "ADMIN,USER,AGENT" }
 #  properties { "datasource-jndi=jdbc/#{kthfs_db}:group-table=USERS_GROUPS:user-table=USERS:group-name-column=groupname:digest-algorithm=none:user-name-column=EMAIL:encoding=Hex:db-user=root:db-password=kthfs"}
 # end

# See http://docs.oracle.com/cd/E26576_01/doc.312/e24938/create-auth-realm.htm


 bash "jdbc_auth_realm" do
   user node['glassfish']['user']
   group node['glassfish']['group']
#   ignore_failure true
   code <<-EOF
   echo "Creating jdbc realm"
   #{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd create-auth-realm --classname com.sun.enterprise.security.auth.realm.jdbc.JDBCRealm --property "jaas-context=jdbcRealm:datasource-jndi=jdbc/#{kthfs_db}:group-table=USERS_GROUPS:user-table=USERS:group-name-column=GROUPNAME:digest-algorithm=none:user-name-column=EMAIL:encoding=Hex:password-column=PASSWORD:assign-groups=ADMIN,USER,AGENT:group-table-user-name-column=EMAIL:digestrealm-password-enc-algorithm= :db-user=root:db-password=kthfs" DBRealm
#{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd set server-config.security-service.default-realm=DBRealm 

# Make sure mysql connections are validated by Glassfish
#{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd set domain.resources.jdbc-connection-pool.#{kthfs_db}.is-connection-validation-required=true domain.resources.jdbc-connection-pool.#{kthfs_db}.is-connection-validation-required=true

# chmod +w ../domains/domain1/config/logging.properties 
# #{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd set-log-level javax.enterprise.system.core.security=FINEST
   EOF
   not_if "#{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd list-auth-realms | grep DBRealm"
 end


kthfsmgr_url = node['kthfs']['mgr']
kthfsmgr_filename = File.basename(kthfsmgr_url)
cached_kthfsmgr_filename = "#{Chef::Config[:file_cache_path]}/#{kthfsmgr_filename}"

Chef::Log.info "Downloading #{cached_kthfsmgr_filename} from #{kthfsmgr_url} "

remote_file cached_kthfsmgr_filename do
    source kthfsmgr_url
    mode 00755
    owner node['glassfish']['user']
    group node['glassfish']['group']
    mode '0600'
    action :create_if_missing
end

Chef::Log.info "Installing KTHFS Dashboard "

bash "unpack_kthfsmgr" do
  user node['glassfish']['user']
  group node['glassfish']['group']
 code <<-EOF
   #{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd deploy --force=true --name KTHFSDashboard #{cached_kthfsmgr_filename}

// This command prevents jclouds client threads (that take a long time to install chef) from timing out, before they have installed their software.
   #{node['glassfish']['base_dir']}/glassfish/bin/asadmin set server-config.network-config.protocols.protocol.http-listener-1.http.request-timeout-seconds=1200

#  --keepstate=true
 EOF
  not_if "#{node['glassfish']['base_dir']}/glassfish/bin/asadmin -u admin -W #{node['glassfish']['base_dir']}/glassfish/domains/domain1_admin_passwd list-applications --type ejb | grep -w 'KTHFSDashboard'"
end
