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

directory node[:ndb][:data_dir] do
  owner node[:ndb][:user]
  mode "0755"
  action :create
end

@id = 0
@found = false
for ndbd in node[:ndb][:data_nodes]
  if node['hostname'].eql? @ndbd
    @found = true
  end
  @id += 1
end 

if @found != true
  return -1
end


for script in node[:ndb][:scripts]
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner "root"
    group "root"
    mode 0655
    variables({
       :ndb_dir => node[:ndb][:base_dir],
       :mysql_dir => node[:mysql][:base_dir],
       :connect_string => node[:ndb][:connect_string],
       :node_id => @id
    })
  end
end 


# create symbolic link from /var/lib/mysql-cluster/ndb-* to 'ndb'. Same for /usr/local/mysql-* to mysql
# Symbolic link is by kthfs-agent to stop/start ndbds, invoke programs
