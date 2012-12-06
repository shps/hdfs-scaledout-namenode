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

  parameters = [:connectiondefinition, :raname, :transactionsupport] +
    ::Chef::Resource::GlassfishConnectorConnectionPool::STRING_ATTRIBUTES +
    ::Chef::Resource::GlassfishConnectorConnectionPool::NUMERIC_ATTRIBUTES +
    ::Chef::Resource::GlassfishConnectorConnectionPool::BOOLEAN_ATTRIBUTES

  command = []
  command << "create-connector-connection-pool"
  parameters.each do |key|
    command << "--#{key}=#{new_resource.send(key)}" if new_resource.send(key)
  end

  command << "--property" << encode_parameters(new_resource.properties) unless new_resource.properties.empty?
  command << "--description" << "'#{new_resource.description}'" if new_resource.description
  command << new_resource.pool_name


  bash "asadmin_create-connector-connection-pool #{new_resource.pool_name}" do
    not_if "#{asadmin_command('list-connector-connection-pools')} | grep -x -- '#{new_resource.pool_name}'"
    user node['glassfish']['user']
    group node['glassfish']['group']
    code asadmin_command(command.join(' '))
  end
end

notifying_action :delete do
  command = []
  command << "delete-connector-connection-pool"
  command << "--target" << new_resource.target if new_resource.target
  command << "--cascade=true"
  command << new_resource.pool_name

  bash "asadmin_delete-connector-connection-pool #{new_resource.pool_name}" do
    only_if "#{asadmin_command('list-connector-connection-pools')} | grep -x -- '#{new_resource.pool_name}'"
    user node['glassfish']['user']
    group node['glassfish']['group']
    code asadmin_command(command.join(' '))
  end
end
