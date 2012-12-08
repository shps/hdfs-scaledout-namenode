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


user node[:ndb][:user] do
  action :create
  system true
  shell "/bin/bash"
end

directory node[:ndb][:mysql_server_dir] do
  owner node[:ndb][:user]
  mode "0755"
  action :create
  recursive true  
end

directory node[:ndb][:mysql_data_dir] do
  owner node[:ndb][:user]
  mode "0755"
  action :create
  recursive true  
end

directory node[:mysql][:base_dir] do
  owner node[:ndb][:user]
  mode "0755"
  action :create
  recursive true
end

directory node[:ndb][:log_dir] do
  mode 0755
  owner node[:ndb][:user]
  action :create
  recursive true
end

remote_file "#{Chef::Config[:file_cache_path]}/ndb.tar.gz" do
  source "http://dev.mysql.com/get/Downloads/MySQL-Cluster-7.2/mysql-cluster-gpl-7.2.8-linux2.6-x86_64.tar.gz/from/http://ftp.sunet.se/pub/unix/databases/relational"
  action :create_if_missing
end

template "mysql.cnf" do
  path "#{node[:ndb][:mysql_server_dir]}/my.cnf"
  source "my.cnf.erb"
  owner "root"
  group "root"
  mode "0644"
  notifies :restart, resources(:service => "mysql")
end

for script in node[:mysql][:scripts]
  template "#{node[:ndb][:base_dir]}/scripts/#{script}" do
    source "#{script}.erb"
    owner "root"
    group "root"
    mode 0644
    variables({
       :ndb_dir => node[:ndb][:base_dir],
       :mysql_dir => node[:mysql][:base_dir],
       :connect_string => node[:ndb][:connect_string],
       :node_id => node[:ndb][:id]
    })
  end
end 



# load the users using distributed privileges
# http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
# mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql
