#
# Cookbook Name:: collectd
# Recipe:: server
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

include_recipe "collectd"

node.normal[:collectd][:config]="collectd-server"

directory "/etc/collectd/#{node[:collectd][:config]}-plugins" do
  owner "root"
  group "root"
  mode "755"
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

directory "/etc/collectd/#{node[:collectd][:config]}-plugins" do
  owner "root"
  group "root"
  mode "755"
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

template "/etc/collectd/#{node[:collectd][:config]}.conf" do
  source "#{node[:collectd][:config]}.conf.erb"
  owner "root"
  group "root"
  mode "644"
  notifies :enable, resources(:service => "#{node[:collectd][:config]}")
  notifies :restart, resources(:service => "#{node[:collectd][:config]}")
end

%w(collection thresholds).each do |file|
  template "/etc/collectd/#{file}.conf" do
    source "#{file}.conf.erb"
    owner "root"
    group "root"
    mode "644"
  end
end

collectd_plugin "network" do
  options :listen=>'0.0.0.0'
  dir "#{node[:collectd][:config]}"
end

collectd_plugin "rrdtool" do
  options :data_dir=>"/var/lib/collectd/rrd"
  dir "#{node[:collectd][:config]}"
end

# bash "create_jarmon_rrd_symbolic_link" do
#     code <<-EOF
# ln -s #{node[:collectd][:data_dir]} /usr/local/glassfish-3.1.2.2/glassfish/domains/domain1/applications/KTHFSDashboard/jarmon/data
# EOF
#   not_if { ::File.exists?( '/usr/local/glassfish-3.1.2.2/glassfish/domains/domain1/applications/KTHFSDashboard/jarmon/data' ) } 
# end



