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
  command << "create-resource-adapter-config"

  command << "--threadpoolid" << new_resource.threadpoolid if new_resource.threadpoolid
  command << "--objecttype" << new_resource.objecttype if new_resource.objecttype

  command << "--property" << encode_parameters(new_resource.properties) unless new_resource.properties.empty?
  command << "--target" << new_resource.target if new_resource.target
  command << new_resource.resource_adapter_name

  bash "asadmin_create-resource-adapter-config #{new_resource.resource_adapter_name}" do
    not_if "#{asadmin_command('list-resource-adapter-configs')} | grep -x -- '#{new_resource.resource_adapter_name}'"
    user node['glassfish']['user']
    group node['glassfish']['group']
    code asadmin_command(command.join(' '))
  end
end

notifying_action :delete do
  command = []
  command << "delete-resource-adapter-config"
  command << "--target" << new_resource.target if new_resource.target
  command << new_resource.resource_adapter_name

  bash "asadmin_delete-resource-adapter-config #{new_resource.resource_adapter_name}" do
    only_if "#{asadmin_command('list-resource-adapter-configs')} | grep -x -- '#{new_resource.resource_adapter_name}'"
    user node['glassfish']['user']
    group node['glassfish']['group']
    code asadmin_command(command.join(' '))
  end
end
