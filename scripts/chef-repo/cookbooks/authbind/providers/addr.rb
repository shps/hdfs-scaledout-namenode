#
# Copyright 2012, Peter Donald, John Whitley
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

action :add do
  file "/etc/authbind/byaddr/#{new_resource.addr}:#{new_resource.port}" do
    owner new_resource.user
    group new_resource.group if new_resource.group
    mode "0550"
    action :create
  end
end

action :remove do
  file "/etc/authbind/byaddr/#{new_resource.addr}:#{new_resource.port}" do
    action :delete
  end
end
