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

# if first_startup
# announce(:ndb, :ndbd)
#
# if bootstrapping
# node[:ndb][:mgm_server][:addr] = discover(:ndb, :mgm_server).private_ip rescue nil
# 

directory node[:ndb][:data_dir] do
  owner node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end

#mgmd_hosts = discover_all(:ndb, :mgm_server).map{|svr| [ svr.node[:k][:zkid], svr.node[:ipaddress] ] }.sort!
#mgmd_host = discover(:ndb, :mgm_server)


case node[:platform_family]
when "debian" # also includes ubuntu in platform_family
  found_id = -1
  id = 1
  for ndbd in node[:ndb][:ndbd][:addrs]
    if node[:ndb][:my_ip].eql? ndbd
      Chef::Log.info "Found matching IP address in the list of data nodes: #{ndbd} . ID= #{id}"
      @found = true
      found_id = id
    end
    id += 1
  end 
  Chef::Log.info "ID IS: #{id}"

  if @found != true
    Chef::Log.fatal "Could not find matching IP address is list of data nodes."
  end

  directory "#{node[:ndb][:data_dir]}/#{found_id}" do
    owner node[:ndb][:user]
    mode "755"
    action :create
    recursive true
  end

end
#when "rhel" # also includes "centos", "redhat", "amazon", "scientific"
# TODO
#end



for script in node[:ndb][:scripts]
  template "#{node[:ndb][:scripts_dir]}/#{script}" do
    source "#{script}.erb"
    owner node[:ndb][:user]
    group node[:ndb][:user]
    mode 0655
    variables({ :node_id => found_id })
  end
end 

service "ndbd" do
  supports :restart => true, :stop => true, :start => true
  action :nothing
end

template "/etc/init.d/ndbd" do
  source "ndbd.erb"
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode 0754
  notifies :enable, resources(:service => "ndbd")
  notifies :restart, resources(:service => "ndbd"), :immediately
end
