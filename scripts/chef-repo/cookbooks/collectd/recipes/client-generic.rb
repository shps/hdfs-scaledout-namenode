#
# Cookbook Name:: collectd
# Recipe:: client
#
# Copyright 2010, Atari, Inc
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

include_recipe "collectd::client"

collectd_plugin "network" do
  options :server=>servers
  :dir=>"#{node[:collectd][:config]}"  
#  options :server=> [#{node[:collectd][:server]}]
end

template "/etc/init.d/#{node[:collectd][:config]}" do
  source "collectd.erb"
  owner "root"
  group "root"
  mode "754"
  variables({
    :config_file => node[:collectd][:config]
  })
end

service "#{node[:collectd][:config]}" do
  supports :restart => true, :status => true
end

%w(node[:collectd][:config] collection thresholds).each do |file|
  template "/etc/collectd/#{file}.conf" do
    source "#{file}.conf.erb"
    owner "root"
    group "root"
    mode "644"
    notifies :enable, resources(:service => "#{node[:collectd][:config]}")
    notifies :restart, resources(:service => "#{node[:collectd][:config]}")
  end
end
