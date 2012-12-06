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


#install ruby
#\curl -kL https://get.rvm.io | bash -s stable --ruby
\curl -kL https://get.rvm.io | sudo bash -s stable
source /etc/profile
rvm install 1.9.2
rvm use 1.9.2

package "build-essential" do
  action :install
end

user node[:ndb][:user] do
  action :create
  system true
  shell "/bin/false"
end

directory node[:ndb][:dir] do
  owner "root"
  mode "0755"
  action :create
end

directory node[:ndb][:data_dir] do
  owner "ndb"
  mode "0755"
  action :create
end

directory node[:ndb][:log_dir] do
  mode 0755
  owner node[:ndb][:user]
  action :create
end

remote_file "#{Chef::Config[:file_cache_path]}/ndb.tar.gz" do
  source "http://dev.mysql.com/get/Downloads/MySQL-Cluster-7.2/mysql-cluster-gpl-7.2.8-linux2.6-x86_64.tar.gz/from/http://ftp.sunet.se/pub/unix/databases/relational"
  action :create_if_missing
end

service "ndb" do
  provider Chef::Provider::Service::Upstart
  subscribes :restart, resources(:bash => "compile_ndb_source")
  supports :restart => true, :start => true, :stop => true
end

template "ndb.conf" do
  path "#{node[:ndb][:dir]}/config.ini"
  source "ndb.conf.erb"
  owner "root"
  group "root"
  mode "0644"
  notifies :restart, resources(:service => "ndb")
end

template "ndb.upstart.conf" do
  path "/etc/init/mysqlcluster.upstart"
  source "ndb.upstart.conf.erb"
  owner "root"
  group "root"
  mode "0644"
  notifies :restart, resources(:service => "ndb")
end

service "ndb" do
  action [:enable, :start]
end

# load the users using distributed privileges
# http://dev.mysql.com/doc/refman/5.5/en/mysql-cluster-privilege-distribution.html
# mysql options -uroot mysql < /usr/local/mysql/share/ndb_dist_priv.sql
