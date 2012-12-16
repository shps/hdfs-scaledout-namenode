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

default['glassfish']['user'] = "glassfish"
default['glassfish']['group'] = "glassfish-admin"

version = "3.1.2.2"
#default['glassfish']['package_url'] = "http://dlc.sun.com.edgesuite.net/glassfish/#{version}/release/glassfish-#{version}.zip"
default['glassfish']['package_url'] = "http://lucan.sics.se/kthfs/glassfish-#{version}.zip"
default['glassfish']['base_dir'] = "/usr/local/glassfish-#{version}"
default['glassfish']['domains_dir'] = "/usr/local/glassfish-#{version}/glassfish/domains"

default['kthfs']['mgr'] = 'http://lucan.sics.se/kthfs/KTHFSDashboard.war'
default['bind_address'] = attribute?('cloud') ? cloud['local_ipv4'] : ipaddress
