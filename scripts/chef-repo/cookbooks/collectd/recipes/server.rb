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

collectd_plugin "network" do
  options :listen=>'0.0.0.0'
end

# hamid
collectd_plugin "rrdtool" do
  options :data_dir=>"/var/lib/collectd/rrd"
end


bash "create_jarmon_rrd_symbolic_link" do
    code <<-EOF
ln -s #{node[:collectd][:data_dir]} /usr/local/glassfish-3.1.2.2/glassfish/domains/domain1/applications/KTHFSDashboard/jarmon/data
EOF
  not_if { ::File.exists?( '/usr/local/glassfish-3.1.2.2/glassfish/domains/domain1/applications/KTHFSDashboard/jarmon/data' ) } 
end


