#
# Cookbook Name:: ndb
# Recipe:: default
#
# Copyright 2012, Example Com
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

include_recipe "ndb"

directory "#{node[:ndb][:base_dir]}/mysql/data" do
  owner node[:ndb][:user]
  mode "0755"
  action :create
  recursive true  
end

template "mysql.cnf" do
  path "#{node[:ndb][:base_dir]}/my.cnf"
  source "my.cnf.erb"
  owner "root"
  group "root"
  mode "0644"
  notifies :restart, resources(:service => "mysql")
end

for script in node[:mysql][:scripts]
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner "root"
    group "root"
    mode 0644
    variables({
       :user => node[:ndb][:user],
       :ndb_dir => node[:ndb][:base_dir],
       :mysql_dir => node[:mysql][:base_dir],
       :ndb_mysql_dir => node[:ndb][:mysql_dir],
       :ndb_mysql_data_dir => node[:ndb][:mysql_data_dir],
       :connect_string => node[:ndb][:connect_string],
       :mysql_port => node[:ndb][:mysql_port],
       :mysql_socket => node[:ndb][:mysql_socket]
    })
  end
end 

# load the users using distributed privileges
# http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
# mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql

# mysql_install_db --config=my.cnf...



args = "[mysqld]"
args << "status"
args << "instance = "
args << "service-group = mysqld"
args << "stop-script = "
args << "start-script = "
args << "pid-file =  "
args << "stdout-file =  "
args << "stderr-file =  "
args << "start-time = " 

bash "install_mysqld_agent" do
  code <<-EOF
   echo args >> node[:kthfs][:base_dir]/services
not_if 
EOF
end
