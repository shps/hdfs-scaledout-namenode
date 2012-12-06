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

include Chef::Asadmin

notifying_action :create do

  command = []
  command << "create-jdbc-resource"
  command << "--connectionpoolid" << new_resource.connectionpoolid
  command << "--property" << encode_parameters(new_resource.properties) unless new_resource.properties.empty?
  command << "--description" << "'#{new_resource.description}'" if new_resource.description
  command << "--enabled=#{enabled}" if new_resource.enabled
  command << "--target" << new_resource.target if new_resource.target
  command << new_resource.name

  bash "asadmin_create_jdbc_resource #{new_resource.name}" do
    not_if "#{asadmin_command('list-jdbc-resources')} | grep -x -- '#{new_resource.name}'"
    user node['glassfish']['user']
    group node['glassfish']['group']
    code asadmin_command(command.join(' '))
  end
end

notifying_action :delete do
  command = []
  command << "delete-jdbc-resource"
  command << "--target" << new_resource.target if new_resource.target
  command << new_resource.name

  bash "asadmin_delete_jdbc_resource #{new_resource.name}" do
    only_if "#{asadmin_command('list-jdbc-resources')} | grep -x -- '#{new_resource.name}'"
    user node['glassfish']['user']
    group node['glassfish']['group']
    code asadmin_command(command.join(' '))
  end
end
