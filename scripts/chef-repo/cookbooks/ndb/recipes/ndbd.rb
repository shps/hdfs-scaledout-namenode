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

Chef::Log.info "Hostname is: #{node['hostname']}"
Chef::Log.info "IP address is: #{node['ipaddress']}"

directory node[:ndb][:data_dir] do
  owner node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end

found_id = -1
id = 1
# if no default IP is set, then look around for the IP 
if node.attribute?('ipaddress') != true
  Chef::Log.warn "Node has no default IP address specified!"
  # just guess what the IP is here
  ipaddress_eth1 = host["network"]["interfaces"]["eth1"]["addresses"].select { |address, data| data["family"] == "inet" }[0][0]
  ipaddress_eth0 = host["network"]["interfaces"]["eth0"]["addresses"].select { |address, data| data["family"] == "inet" }[0][0]
  for ndbd in node[:ndb][:data_nodes]
    Chef::Log.info "Testing IP address: #{ndbd}"
    if ipaddress_eth1.eql? ndbd
      @found = true
      found_id = id
    end
    if ipaddress_eth0.eql? ndbd
      @found = true
      found_id = id
    end
    id += 1
  end 
else
# default IP is set, here
  for ndbd in node[:ndb][:data_nodes]
    Chef::Log.info "Testing IP address: #{ndbd}"
    if node['ipaddress'].eql? ndbd
      Chef::Log.info "Found matching IP address in the list of data nodes: #{ndbd} . ID= #{id}"
      @found = true
      found_id = id
    end
    id += 1
  end 
end
  Chef::Log.info "ID IS: #{id}"

  if @found != true
    Chef::Log.info "Could not find matching IP address is list of data nodes."
  end

directory "#{node[:ndb][:data_dir]}/#{found_id}" do
  owner node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end


for script in node[:ndb][:scripts]
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner node[:ndb][:user]
    group node[:ndb][:user]
    mode 0655
    variables({
                :ndb_dir => node[:ndb][:base_dir],
                :mysql_dir => node[:mysql][:base_dir],
                :connect_string => node[:ndb][:connect_string],
                :node_id => found_id
              })
  end
end 

template "/etc/init.d/ndbd" do
  source "ndbd.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0655
  variables({
              :ndb_dir => node[:ndb][:base_dir],
              :mysql_dir => node[:mysql][:base_dir],
              :connect_string => node[:ndb][:connect_string]
            })
  notifies :enable, resources(:service => "ndbd")
end

service "ndbd" do
  supports :restart => true, :stop => true, :start => true
  action :start, :immediately
end
