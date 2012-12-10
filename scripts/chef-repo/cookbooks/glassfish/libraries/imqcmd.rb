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

class Chef
  module Imqcmd
    def imqcmd_command(command)
      args = []
      args << "-f"
      args << "-javahome #{node["java"]["java_home"]}"
      args << "-b #{new_resource.host}:#{new_resource.port}"
      args << "-u #{new_resource.username}"
      args << "-passfile #{new_resource.passfile}"
      "#{node['glassfish']['base_dir']}/mq/bin/imqcmd #{args.join(" ")} #{command}"
    end
  end
end

