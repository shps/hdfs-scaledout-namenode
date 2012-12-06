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

actions :deploy, :undeploy, :enable, :disable

attribute :component_name, :kind_of => String, :name_attribute => true
attribute :version, :kind_of => String, :default => nil
attribute :target, :kind_of => String, :default => nil
attribute :url, :kind_of => String, :required => true
attribute :enabled, :equal_to => [true, false, 'true', 'false'], :default => true
attribute :type, :equal_to => [:osgi, 'osgi', nil], :default => nil
attribute :context_root, :kind_of => String, :default => nil
attribute :virtual_servers, :kind_of => Array, :default => []
attribute :generate_rmi_stubs, :equal_to => [true, false, 'true', 'false'], :default => false
attribute :availability_enabled, :equal_to => [true, false, 'true', 'false'], :default => false
attribute :lb_enabled, :equal_to => [true, false, 'true', 'false'], :default => true
attribute :keep_state, :equal_to => [true, false, 'true', 'false', ], :default => false
attribute :verify, :equal_to => [true, false, 'true', 'false'], :default => false
attribute :precompile_jsp, :equal_to => [true, false, 'true', 'false'], :default => true
attribute :async_replication, :equal_to => [true, false, 'true', 'false'], :default => true
attribute :properties, :kind_of => Hash, :default => {}
attribute :descriptors, :kind_of => Hash, :default => {}

attribute :domain_name, :kind_of => String, :required => true
attribute :terse, :kind_of => [TrueClass, FalseClass], :default => false
attribute :echo, :kind_of => [TrueClass, FalseClass], :default => true
attribute :username, :kind_of => String, :default => nil
attribute :password_file, :kind_of => String, :default => nil
attribute :secure, :kind_of => [TrueClass, FalseClass], :default => true
attribute :admin_port, :kind_of => Integer, :default => 4848

def initialize( *args )
  super
  @action = :deploy
end
